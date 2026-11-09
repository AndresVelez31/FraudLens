<#
.SYNOPSIS
  Demo guiada de FraudLens sobre el ambiente local (docker compose).

.DESCRIPTION
  Recorre el sistema de punta a punta, con una pausa antes de cada paso para explicarlo:
    1. Estado de los servicios
    2. Carga de catálogos (DAG catalogos_diario)
    3. Tráfico de pagos con ráfagas de fraude (R1) y registros dañados
    4. Alertas en la API
    5. Casos conocidos de R3 y R2
    6. Capas del lakehouse (Bronze, Silver, Gold)
    7. Preguntas de negocio P4 a P6
    8. Tolerancia a fallos: un broker caído y el streaming reiniciado
  Al final muestra un resumen con lo que se esperaba y lo que pasó.

.PARAMETER SinPausas
  Corre todo seguido, sin esperar Enter entre pasos (útil para ensayar o probar).

.PARAMETER SinFallos
  Omite el paso 8 (tolerancia a fallos).

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts\demo.ps1
.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts\demo.ps1 -SinPausas
#>
param(
    [switch]$SinPausas,
    [switch]$SinFallos
)

$ErrorActionPreference = "Continue"
$Raiz = Split-Path -Parent $PSScriptRoot
Set-Location $Raiz
$Api = "http://localhost:8080"
$Resumen = New-Object System.Collections.ArrayList

# ------------------------------------------------------------------ utilidades
function Titulo($numero, $texto, $explicacion) {
    Write-Host ""
    Write-Host ("=" * 78) -ForegroundColor DarkCyan
    Write-Host (" PASO {0}. {1}" -f $numero, $texto) -ForegroundColor Cyan
    Write-Host ("=" * 78) -ForegroundColor DarkCyan
    if ($explicacion) { Write-Host $explicacion -ForegroundColor Gray }
    if (-not $SinPausas) { [void](Read-Host "`nEnter para ejecutar") }
}

function Info($texto) { Write-Host "  $texto" -ForegroundColor White }
function Ok($texto)   { Write-Host "  [OK] $texto" -ForegroundColor Green }
function Mal($texto)  { Write-Host "  [!!] $texto" -ForegroundColor Yellow }

function Anotar($prueba, $esperado, $obtenido, [bool]$bien) {
    [void]$Resumen.Add([pscustomobject]@{
        Prueba = $prueba; Esperado = $esperado; Obtenido = $obtenido
        Resultado = $(if ($bien) { "OK" } else { "REVISAR" })
    })
    if ($bien) { Ok "$prueba : $obtenido" } else { Mal "$prueba : se esperaba $esperado y se obtuvo $obtenido" }
}

function Alertas($regla) {
    try {
        return [int](Invoke-RestMethod -Uri "$Api/alertas?regla=$regla&limite=500" -TimeoutSec 15).total
    } catch { return -1 }
}

function IdsAlertas() {
    $ids = @()
    foreach ($r in "R1", "R2", "R3") {
        try { $ids += (Invoke-RestMethod -Uri "$Api/alertas?regla=$r&limite=500" -TimeoutSec 15).alertas.id_alerta } catch {}
    }
    return $ids
}

function Productor([string[]]$argumentos) {
    # --no-deps: el ambiente ya está arriba; sin esto, compose volvería a encender un broker apagado a propósito.
    $salida = docker compose run --rm --no-deps producer python -m producer.producer @argumentos 2>&1 | ForEach-Object { "$_" }
    $salida | Where-Object { $_ -match "inyectado|entregados|tráfico normal" } | ForEach-Object { Info $_ }
    $linea = $salida | Where-Object { $_ -match "entregados=" } | Select-Object -Last 1
    return $linea
}

function JsonAirflow([string[]]$argumentos) {
    $texto = (docker compose exec -T airflow-scheduler airflow @argumentos -o json 2>$null) -join "`n"
    $inicio = $texto.IndexOf("[")
    if ($inicio -lt 0) { return @() }
    try { return ($texto.Substring($inicio) | ConvertFrom-Json) } catch { return @() }
}

function CorrerDag($dag, [int]$minutos = 10) {
    $antes = @(JsonAirflow @("dags", "list-runs", "-d", $dag)).Count
    docker compose exec -T airflow-scheduler airflow dags trigger $dag 2>$null | Out-Null
    Info "DAG $dag lanzado; esperando a que termine (máximo $minutos min)..."
    $limite = (Get-Date).AddMinutes($minutos)
    while ((Get-Date) -lt $limite) {
        Start-Sleep -Seconds 10
        $corridas = @(JsonAirflow @("dags", "list-runs", "-d", $dag))
        if ($corridas.Count -gt $antes) {
            $ultima = $corridas | Sort-Object { [datetime]$_.execution_date } -Descending | Select-Object -First 1
            if ($ultima.state -in @("success", "failed")) { return $ultima.state }
        }
    }
    return "sin terminar"
}

function EsperarAlertas($regla, [int]$minimo, [int]$segundos = 90) {
    $limite = (Get-Date).AddSeconds($segundos)
    do {
        $n = Alertas $regla
        if ($n -ge $minimo) { return $n }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $limite)
    return $n
}

# ------------------------------------------------------------------ 1. servicios
Titulo 1 "Estado de los servicios" @"
Cada pieza del pipeline corre en su contenedor: 3 brokers de Kafka, el job de Spark Structured Streaming,
PostgreSQL, Airflow, la API, y MinIO y DynamoDB Local haciendo de S3 y DynamoDB.
"@
$corriendo = @(docker compose ps --services --status running 2>$null)
if ($corriendo.Count -lt 10) {
    Mal "Hay $($corriendo.Count) servicios corriendo; se levanta el ambiente (la primera vez tarda varios minutos)."
    docker compose up -d --build
    Start-Sleep -Seconds 30
    $corriendo = @(docker compose ps --services --status running 2>$null)
}
docker compose ps --format "table {{.Service}}`t{{.State}}`t{{.Status}}"
try { $salud = Invoke-RestMethod -Uri "$Api/salud" -TimeoutSec 15 } catch { $salud = $null }
Anotar "Servicios corriendo" ">= 10" $corriendo.Count ($corriendo.Count -ge 10)
Anotar "API /salud" "ok" $(if ($salud) { $salud.estado } else { "sin respuesta" }) ([bool]$salud)
$inicioR1 = Alertas "R1"; $inicioR2 = Alertas "R2"; $inicioR3 = Alertas "R3"
Info "Alertas que ya había: R1=$inicioR1  R2=$inicioR2  R3=$inicioR3"

# ------------------------------------------------------------------ 2. catálogos
Titulo 2 "Carga de catálogos (Airflow: catalogos_diario)" @"
Lleva clientes y comercios de PostgreSQL al lakehouse (Bronze y Silver, con los datos personales seudonimizados
con HMAC) y carga la lista de riesgo que usa la regla R3. Se puede ver en http://localhost:8081 (admin / admin).
"@
$estado = CorrerDag "catalogos_diario" 10
Anotar "DAG catalogos_diario" "success" $estado ($estado -eq "success")

# ------------------------------------------------------------------ 3. tráfico
Titulo 3 "Tráfico de pagos durante 60 segundos" @"
El productor publica 50 pagos por segundo en Kafka (clave = id_cliente). Cada 20 segundos mete una ráfaga de
fraude de un cliente (8 pagos seguidos, regla R1) y 5 registros dañados: 1 JSON ilegible (va a la DLQ) y 4 con
campos inválidos (van a cuarentena con su motivo).
"@
$linea = Productor @("--escenario", "mixto", "--rate", "50", "--duracion", "60")
Anotar "Pagos entregados a Kafka" "errores=0" $linea ($linea -match "errores=0")

# ------------------------------------------------------------------ 4. alertas
Titulo 4 "Alertas de ráfaga (R1) en la API" @"
Spark revisa ventanas deslizantes de 5 minutos por cliente y alerta si hay más de 5 pagos o más de 2.000.000.
Las alertas quedan en DynamoDB y se consultan por la API: $Api/docs
"@
$r1 = EsperarAlertas "R1" ($inicioR1 + 1) 90
Anotar "Alertas R1 nuevas" "> 0" ($r1 - $inicioR1) ($r1 -gt $inicioR1)
try {
    $muestra = (Invoke-RestMethod -Uri "$Api/alertas?regla=R1&limite=3" -TimeoutSec 15).alertas
    $muestra | Select-Object regla, id_cliente, n_tx, monto_total, ts_evento, ts_alerta | Format-Table -AutoSize | Out-String | Write-Host
} catch {}
Info "En el navegador: $Api/docs -> GET /alertas -> Try it out"

# ------------------------------------------------------------------ 5. R3 y R2
Titulo 5 "Casos con respuesta conocida: R3 (lista de riesgo) y R2 (dispositivo nuevo)" @"
R3: un cliente paga a los 10 comercios de riesgo y desde los 10 dispositivos de riesgo. La lista v1 tiene 5 de
cada uno, así que deben salir exactamente 10 alertas.
R2: se crea un historial de 10 días para 5 clientes, Airflow calcula su promedio de 30 días (Gold), y luego cada
uno paga 6 veces su promedio desde un dispositivo nunca visto. Deben salir exactamente 5 alertas.
"@
Info "--- R3"
Productor @("--escenario", "r3") | Out-Null
$r3 = EsperarAlertas "R3" ($inicioR3 + 10) 90
Anotar "Alertas R3 nuevas" "10" ($r3 - $inicioR3) (($r3 - $inicioR3) -eq 10)

Info "--- R2: historial de los clientes"
Productor @("--escenario", "r2-base") | Out-Null
Start-Sleep -Seconds 20
Info "--- R2: Gold calcula el perfil de 30 días (fraude_pipeline)"
$estado = CorrerDag "fraude_pipeline" 12
Anotar "DAG fraude_pipeline" "success" $estado ($estado -eq "success")
Info "--- R2: pagos desde dispositivos nuevos"
Productor @("--escenario", "r2") | Out-Null
$r2 = EsperarAlertas "R2" ($inicioR2 + 5) 90
Anotar "Alertas R2 nuevas" "5" ($r2 - $inicioR2) (($r2 - $inicioR2) -eq 5)

# ------------------------------------------------------------------ 6. lakehouse
Titulo 6 "Capas del lakehouse" @"
Bronze guarda el evento crudo tal como llegó; Silver, los datos validados y sin duplicados (más la cuarentena);
Gold, el esquema en estrella para las preguntas de negocio. Se puede navegar en http://localhost:9001
(minioadmin / minioadmin).
"@
$codigo = @'
import os, collections, boto3
s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"], region_name=os.environ["AWS_REGION"])
tablas = collections.Counter()
for p in s3.get_paginator("list_objects_v2").paginate(Bucket=os.environ["S3_BUCKET"]):
    for o in p.get("Contents", []):
        partes = o["Key"].split("/")
        if len(partes) > 2 and partes[0] in ("bronze", "silver", "gold") and o["Key"].endswith(".parquet"):
            tablas[partes[0] + "/" + partes[1]] += 1
for t in sorted(tablas):
    print("  %-32s %4d archivos parquet" % (t, tablas[t]))
'@
$codigo | docker compose run --rm -T --no-deps streaming python - 2>$null
Info "Ver en el navegador: http://localhost:9001"

# ------------------------------------------------------------------ 7. P4 a P6
Titulo 7 "Preguntas de negocio sobre Gold (P4, P5 y P6)" @"
P4: tasa de fraude por comercio, ciudad y franja horaria, semana a semana.
P5: latencia promedio y P95 entre el pago y la alerta, y alertas por regla.
P6: los 10 comercios con más monto sospechoso en los últimos 7 días.
En AWS estas mismas consultas corren en Athena; aquí corren con Spark SQL sobre las mismas tablas.
"@
docker compose run --rm --no-deps streaming python -m batch.consultas todas --filas 10 2>$null |
    Where-Object { $_ -notmatch "WARN|Setting default log level|setLogLevel" }

# ------------------------------------------------------------------ 8. fallos
if (-not $SinFallos) {
    Titulo 8 "Tolerancia a fallos" @"
a) Se apaga un broker de Kafka: con 3 brokers y min.insync.replicas=2 el sistema sigue recibiendo pagos.
b) Se mata el job de streaming a la fuerza: al volver, retoma desde su checkpoint y no duplica alertas.
"@
    Info "--- a) broker kafka-3 apagado"
    docker compose stop kafka-3 | Out-Null
    $linea = Productor @("--escenario", "r1")
    Anotar "Pagos con un broker caído" "errores=0" $linea ($linea -match "errores=0")
    docker compose start kafka-3 | Out-Null
    Info "kafka-3 encendido de nuevo"

    Info "--- b) streaming detenido con kill -9"
    $antes = IdsAlertas
    docker compose kill -s KILL streaming | Out-Null
    Productor @("--escenario", "normal", "--rate", "20", "--duracion", "10") | Out-Null
    docker compose up -d streaming | Out-Null
    Info "Streaming reiniciado; esperando a que procese lo que quedó en Kafka..."
    Start-Sleep -Seconds 75
    $despues = IdsAlertas
    $duplicadas = $despues.Count - @($despues | Select-Object -Unique).Count
    $perdidas = @($antes | Where-Object { $despues -notcontains $_ }).Count
    Anotar "Alertas duplicadas tras el reinicio" "0" $duplicadas ($duplicadas -eq 0)
    Anotar "Alertas perdidas tras el reinicio" "0" $perdidas ($perdidas -eq 0)
}

# ------------------------------------------------------------------ resumen
Write-Host ""
Write-Host ("=" * 78) -ForegroundColor DarkCyan
Write-Host " RESUMEN DE LA DEMO" -ForegroundColor Cyan
Write-Host ("=" * 78) -ForegroundColor DarkCyan
$Resumen | Format-Table -AutoSize | Out-String | Write-Host
$fallas = @($Resumen | Where-Object { $_.Resultado -ne "OK" }).Count
if ($fallas -eq 0) { Ok "Todo salió como se esperaba." } else { Mal "$fallas prueba(s) para revisar: docker compose logs <servicio>" }
Write-Host ""
Info "Airflow:  http://localhost:8081   (admin / admin)"
Info "API:      $Api/docs"
Info "MinIO:    http://localhost:9001   (minioadmin / minioadmin)"
Info "Apagar:   docker compose down     (docker compose down -v borra todos los datos)"
exit $fallas

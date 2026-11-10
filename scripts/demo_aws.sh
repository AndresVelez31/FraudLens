#!/usr/bin/env bash
# Demo en vivo de FraudLens en AWS, manejada desde el portátil (Git Bash o Linux) con el AWS CLI y Systems Manager.
# No usa SSH ni llaves: toma la sesión del perfil de AWS (aws login --profile fraudlens).
#
#   bash scripts/demo_aws.sh encender           # enciende las dos EC2 y espera a que respondan por SSM (~3 min)
#   bash scripts/demo_aws.sh preparar           # deja main, NOTIFICAR_DESDE=lambda y el DAG activo (ensayo, ~10 min)
#   bash scripts/demo_aws.sh estado             # contenedores de las dos EC2 y últimas corridas del DAG
#   bash scripts/demo_aws.sh reservar [N]       # saca del sistema los últimos N pagos del dataset (def. 2000; antes de la demo)
#   bash scripts/demo_aws.sh enviar [N]         # envía esos N pagos: llegan de verdad, con su tasa real de alertas
#   bash scripts/demo_aws.sh repetir [N]        # reenvía N pagos ya cargados: Silver no crece (idempotencia)
#   bash scripts/demo_aws.sh limpiar SUFIJO     # borra pagos de prueba enviados con un sufijo en el id
#   bash scripts/demo_aws.sh streaming          # progreso del replayer y los últimos micro-lotes de Spark
#   bash scripts/demo_aws.sh alertas [regla]    # últimas alertas por regla desde la API (def. R4)
#   bash scripts/demo_aws.sh gold               # corre el DAG fraude_pipeline y muestra el estado de cada tarea
#   bash scripts/demo_aws.sh consulta [p4|p5|p6|p7|p7r]  # pregunta de negocio sobre Gold en Athena (def. p6)
#   bash scripts/demo_aws.sh airflow            # abre Airflow en http://localhost:8081 (necesita el plugin de SSM)
#   bash scripts/demo_aws.sh apagar             # detiene las dos EC2
#
# Correos: con NOTIFICAR_DESDE=lambda la Lambda manda un correo resumen por lote (junta hasta 5 min), así que 2.000
# pagos dan unas 90 alertas y de 1 a 4 correos. `enviar` debe usar el mismo N que `reservar`.
set -euo pipefail

export AWS_PROFILE="${AWS_PROFILE:-fraudlens}"
export AWS_REGION="${AWS_REGION:-us-east-2}"
EC2_A="${EC2_A:-i-05f1133376ba2ac37}"   # streaming: Kafka, Spark, replayer
EC2_B="${EC2_B:-i-0eba9e79f7ea9e9f6}"   # batch: PostgreSQL, Airflow, API
BUCKET="${BUCKET:-fraudlens-lake-216408407230}"
TABLA="${TABLA:-alertas_fraude}"
RAIZ="$(cd "$(dirname "$0")/.." && pwd)"
TMPD="$(mktemp -d)"; trap 'rm -rf "$TMPD"' EXIT
if command -v cygpath >/dev/null 2>&1; then TMPW="$(cygpath -m "$TMPD")"; else TMPW="$TMPD"; fi   # el AWS CLI de Windows no lee /tmp

# Corre comandos bash en una EC2 por SSM (como root, en /opt/fraudlens) y muestra la salida.
remoto() {
  local inst="$1" cmds="$2" max="${3:-300}" id s t=0
  python -c 'import json,sys; json.dump({"commands": ["cd /opt/fraudlens", sys.argv[1]], "executionTimeout": ["3600"]}, open(sys.argv[2], "w"))' \
    "$cmds" "$TMPD/params.json"
  id=$(aws ssm send-command --instance-ids "$inst" --document-name AWS-RunShellScript \
        --parameters "file://$TMPW/params.json" --query Command.CommandId --output text)
  while :; do
    sleep 3; t=$((t + 3))
    s=$(aws ssm get-command-invocation --command-id "$id" --instance-id "$inst" --query Status --output text 2>/dev/null || echo Pending)
    case "$s" in Success|Failed|Cancelled|TimedOut) break ;; esac
    if [ "$t" -ge "$max" ]; then echo "(sigue corriendo tras ${max}s; comando $id)"; return 0; fi
  done
  aws ssm get-command-invocation --command-id "$id" --instance-id "$inst" \
    --query "[StandardOutputContent,StandardErrorContent]" --output text | sed '/^None$/d'
  [ "$s" = Success ]
}

# Deja la última versión de main en la EC2 sin tocar el .env. Corre como root (SSM), así que se marca el clon como
# seguro; si la actualización falla, el comando se detiene en lugar de levantar el código viejo.
ACTUALIZAR='set -e; export HOME=/root; git config --global --get-all safe.directory | grep -qx /opt/fraudlens || git config --global --add safe.directory /opt/fraudlens; git fetch -q origin; git checkout -q main; git reset -q --hard origin/main; git log --oneline -1'
PONER='poner() { if grep -q "^$1=" .env; then sed -i "s|^$1=.*|$1=$2|" .env; else echo "$1=$2" >> .env; fi; }'
DAG='docker compose exec -T airflow-scheduler airflow'

caso="${1:-ayuda}"; shift || true
case "$caso" in
  encender)
    aws ec2 start-instances --instance-ids "$EC2_A" "$EC2_B" --query "StartingInstances[].[InstanceId,CurrentState.Name]" --output text
    aws ec2 wait instance-running --instance-ids "$EC2_A" "$EC2_B"
    echo "EC2 encendidas; esperando a Systems Manager..."
    for i in $(seq 1 40); do
      n=$(aws ssm describe-instance-information --filters "Key=InstanceIds,Values=$EC2_A,$EC2_B" \
            --query "length(InstanceInformationList[?PingStatus=='Online'])" --output text)
      [ "$n" = 2 ] && break; sleep 10
    done
    echo "SSM en línea: $n de 2. Los contenedores arrancan solos; en 2-3 min corre: bash scripts/demo_aws.sh estado"
    ;;
  preparar)
    echo "== EC2-A (streaming)"
    remoto "$EC2_A" "$ACTUALIZAR; $PONER
poner RULE_VERSION ieee; poner REGLAS_ACTIVAS R2,R3,R4; poner NOTIFICAR_DESDE lambda; poner STARTING_OFFSETS latest
grep -E '^(COMPOSE_PROFILES|RULE_VERSION|REGLAS_ACTIVAS|NOTIFICAR_DESDE|STARTING_OFFSETS)=' .env
docker compose up -d --build 2>&1 | tail -3" 900
    echo "== EC2-B (batch)"
    remoto "$EC2_B" "$ACTUALIZAR; $PONER
poner RULE_VERSION ieee; poner NOTIFICAR_DESDE lambda
docker compose up -d --build 2>&1 | tail -3
sleep 20; $DAG dags unpause fraude_pipeline 2>&1 | tail -1" 900
    ;;
  estado)
    echo "== EC2-A"; remoto "$EC2_A" "git log --oneline -1; docker ps --format '{{.Names}}  {{.Status}}'"
    echo "== EC2-B"; remoto "$EC2_B" "docker ps --format '{{.Names}}  {{.Status}}'
$DAG dags list-runs -d fraude_pipeline -o plain 2>/dev/null | awk 'NR>1 && NR<5 {print \$3, \$5}'"
    ;;
  reservar|limpiar)
    if [ "$caso" = reservar ]; then args="--n ${1:-2000}"; else [ -n "${1:-}" ] || { echo "uso: limpiar SUFIJO"; exit 1; }; args="--sufijo $1"; fi
    remoto "$EC2_B" "set -o pipefail; docker compose exec -T airflow-scheduler python -m batch.demo $caso $args 2>&1 | grep -E 'fraudlens.demo|Error|error' | cut -c1-200" 900
    # Las alertas de esos pagos en DynamoDB se borran desde aquí, con la sesión del equipo (el rol de la EC2 no puede
    # recorrer la tabla). La lista de ids la dejó batch.demo en el lake.
    aws s3 cp --quiet "s3://$BUCKET/demo/ids_demo.txt" "$TMPW/ids.txt"
    aws dynamodb scan --table-name "$TABLA" --projection-expression "id_alerta, id_transaccion" --output json > "$TMPD/scan.json"
    python - "$TMPD/ids.txt" "$TMPD/scan.json" "$TMPD" "$TABLA" <<'PY'
import json, sys
ids = set(open(sys.argv[1], encoding="utf-8").read().split())
items = json.load(open(sys.argv[2], encoding="utf-8"))["Items"]
borrar = [i["id_alerta"]["S"] for i in items if i.get("id_transaccion", {}).get("S") in ids]
for n in range(0, len(borrar), 25):
    pedido = {sys.argv[4]: [{"DeleteRequest": {"Key": {"id_alerta": {"S": a}}}} for a in borrar[n:n + 25]]}
    json.dump(pedido, open("%s/borrar_%04d.json" % (sys.argv[3], n // 25), "w"))
print("DynamoDB: %d alertas de esos pagos (de %d en la tabla)" % (len(borrar), len(items)))
PY
    for f in "$TMPD"/borrar_*.json; do
      [ -e "$f" ] || break
      quedan=$(aws dynamodb batch-write-item --request-items "file://$TMPW/$(basename "$f")" --query "length(UnprocessedItems.*[] || \`[]\`)" --output text)
      [ "$quedan" = 0 ] || echo "aviso: $quedan borrados quedaron pendientes en $(basename "$f")"
    done
    echo "DynamoDB: listo"
    ;;
  enviar|repetir)
    n="${1:-2000}"
    if [ "$caso" = enviar ]; then cuales="--ultimos $n"; else cuales="--limite $n"; fi
    remoto "$EC2_A" "docker rm -f fraudlens-replayer >/dev/null 2>&1 || true
docker compose run -d --no-deps --name fraudlens-replayer producer python -m producer.replayer \
  --particion prueba $cuales --factor 0 --tasa-max 50 --malos-pct 1 2>&1 | tail -1
echo 'replayer en marcha: $n pagos a 50 por segundo ($cuales)'"
    ;;
  streaming)
    remoto "$EC2_A" "echo '-- replayer'; docker logs fraudlens-replayer 2>&1 | grep -E 'enviad|\"enviados\"|error' | tail -2
echo '-- Spark: últimos micro-lotes'; docker logs fraudlens-streaming-1 2>&1 | grep -E 'lote [0-9]+: (raw=|[0-9]+ alertas)' | tail -6 | cut -c1-200
echo \"errores: \$(docker logs --since 30m fraudlens-streaming-1 2>&1 | grep -cE 'ERROR|Exception')\""
    ;;
  alertas)
    regla="${1:-R4}"
    remoto "$EC2_B" "curl -s 'http://localhost:8080/alertas?regla=$regla&limite=5' | python3 -c '
import json, sys
d = json.load(sys.stdin)
print(\"alertas devueltas:\", d.get(\"total\"))
for a in d.get(\"alertas\", [])[:5]:
    print(a.get(\"ts_alerta\"), a.get(\"regla\"), a.get(\"id_transaccion\"), a.get(\"id_cliente\"), str(a.get(\"detalle\") or \"\")[:60])'"
    ;;
  gold)
    remoto "$EC2_B" "$DAG dags trigger fraude_pipeline 2>&1 | tail -1 | cut -c1-120
for i in \$(seq 1 60); do
  s=\$($DAG dags list-runs -d fraude_pipeline -o plain 2>/dev/null | awk 'NR==2{print \$3}')
  [ \"\$s\" = success ] || [ \"\$s\" = failed ] && break; sleep 10
done
echo \"fraude_pipeline: \$s\"
r=\$($DAG dags list-runs -d fraude_pipeline -o plain 2>/dev/null | awk 'NR==2{print \$2}')
$DAG tasks states-for-dag-run fraude_pipeline \"\$r\" -o plain 2>/dev/null | awk 'NR>1{print \$3, \$NF}'" 900
    ;;
  consulta)
    p="${1:-p6}"
    case "$p" in p7r) archivo=p7_acierto_por_regla ;; *) archivo=$(ls "$RAIZ/dashboards/athena" | grep "^${p}_" | head -1 | sed 's/\.sql$//') ;; esac
    [ -n "$archivo" ] || { echo "consulta desconocida: $p"; exit 1; }
    python - "$RAIZ/dashboards/athena/$archivo.sql" "$TMPD/q.json" <<'PY'
import json, sys
q = "\n".join(l for l in open(sys.argv[1], encoding="utf-8").read().splitlines() if not l.strip().startswith("--"))
json.dump({"QueryString": q, "WorkGroup": "fraudlens", "QueryExecutionContext": {"Database": "fraudlens"}}, open(sys.argv[2], "w"))
PY
    echo "== $archivo.sql"
    id=$(aws athena start-query-execution --cli-input-json "file://$TMPW/q.json" --query QueryExecutionId --output text)
    while :; do
      s=$(aws athena get-query-execution --query-execution-id "$id" --query QueryExecution.Status.State --output text)
      case "$s" in SUCCEEDED|FAILED|CANCELLED) break ;; esac; sleep 1
    done
    if [ "$s" != SUCCEEDED ]; then
      aws athena get-query-execution --query-execution-id "$id" --query QueryExecution.Status.StateChangeReason --output text; exit 1
    fi
    aws athena get-query-execution --query-execution-id "$id" \
      --query "QueryExecution.Statistics.[DataScannedInBytes,TotalExecutionTimeInMillis]" --output text \
      | awk '{printf "leído: %.1f MB · tiempo: %.1f s\n", $1/1e6, $2/1000}'
    aws athena get-query-results --query-execution-id "$id" --max-items 16 --output json | python -c '
import json, sys
for r in json.load(sys.stdin)["ResultSet"]["Rows"]:
    print(" | ".join(c.get("VarCharValue", "") for c in r["Data"]))'
    ;;
  airflow)
    command -v session-manager-plugin >/dev/null 2>&1 || {
      echo "Falta el Session Manager plugin del AWS CLI (instalador oficial de AWS). Sin él, usa: bash scripts/demo_aws.sh gold"; exit 1; }
    echo "Airflow en http://localhost:8081 (Ctrl+C para cerrar el túnel)"
    aws ssm start-session --target "$EC2_B" --document-name AWS-StartPortForwardingSession \
      --parameters '{"portNumber":["8081"],"localPortNumber":["8081"]}'
    ;;
  apagar)
    aws ec2 stop-instances --instance-ids "$EC2_A" "$EC2_B" --query "StoppingInstances[].[InstanceId,CurrentState.Name]" --output text
    ;;
  *)
    sed -n '2,17p' "$0"
    ;;
esac

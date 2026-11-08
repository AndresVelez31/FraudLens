"""Prueba de integración de punta a punta SIN Docker (usa Kafka local de 1 broker y un DynamoDB simulado con moto).

Recorre el flujo completo del E2 con respuestas conocidas y falla (exit 1) si algo no cuadra:
  catálogos (CSV) -> Bronze/Silver -> streaming -> Gold -> R1/R2/R3 -> DLQ/cuarentena/duplicados ->
  reinicio forzado (kill -9) sin duplicados -> lista de riesgo v2 -> calidad -> P4-P6 -> reproceso con v2.

Requisitos: Kafka en KAFKA_BOOTSTRAP_SERVERS (tests/kafka_local.sh start), moto en DYNAMODB_ENDPOINT
(`moto_server -p 5001`), SPARK_EXTRA_JARS con los jars de Delta y Kafka, Java 17+.
Uso:  python tests/e2e_local.py [--lake /tmp/fl-lake]
"""
import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

ENV = dict(os.environ)
ENV.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ENV.setdefault("DYNAMODB_ENDPOINT", "http://localhost:5001")
ENV.setdefault("PII_HMAC_KEY", "clave-e2e")
ENV.setdefault("AWS_ACCESS_KEY_ID", "test")
ENV.setdefault("AWS_SECRET_ACCESS_KEY", "test")
ENV.setdefault("AWS_REGION", "us-east-1")
ENV.setdefault("SPARK_MASTER", "local[2]")
ENV.setdefault("TZ", "UTC")
ENV["PYTHONPATH"] = RAIZ
os.environ.update({k: ENV[k] for k in ("DYNAMODB_ENDPOINT", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION")})

FALLOS = []
T0 = time.time()


def log(msg):
    print("[%5.0fs] %s" % (time.time() - T0, msg), flush=True)


def verificar(nombre, condicion, detalle=""):
    log("%s %s %s" % ("OK   " if condicion else "FALLA", nombre, detalle))
    if not condicion:
        FALLOS.append(nombre)


def py(*args, _ok=0, **extra):
    """Corre `python -m ...`. `extra` son variables de entorno; `_ok` es un código de salida aceptado además del 0."""
    env = dict(ENV, **extra)
    r = subprocess.run([sys.executable, "-m", *args], cwd=RAIZ, env=env, capture_output=True, text=True)
    if r.returncode not in (0, _ok):
        print(r.stdout[-3000:], r.stderr[-3000:])
    return r


def tabla(lake, *partes):
    from deltalake import DeltaTable

    return DeltaTable(os.path.join(lake, *partes)).to_pandas()


def esperar(cond, timeout, paso=2, que=""):
    fin = time.time() + timeout
    while time.time() < fin:
        try:
            if cond():
                return True
        except Exception:  # noqa: BLE001
            pass
        time.sleep(paso)
    log("tiempo agotado esperando: %s" % que)
    return False


def alertas():
    import boto3

    t = boto3.resource("dynamodb", region_name="us-east-1", endpoint_url=ENV["DYNAMODB_ENDPOINT"]).Table("alertas_fraude")
    items, kw = [], {}
    while True:
        r = t.scan(**kw)
        items += r["Items"]
        if "LastEvaluatedKey" not in r:
            return items
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]


def por_regla(items):
    out = {}
    for i in items:
        out.setdefault(i["regla"], []).append(i)
    return out


def reiniciar_infra():
    import boto3
    from confluent_kafka.admin import AdminClient, NewTopic

    adm = AdminClient({"bootstrap.servers": ENV["KAFKA_BOOTSTRAP_SERVERS"]})
    existentes = set(adm.list_topics(timeout=10).topics)
    borrar = [t for t in ("transacciones", "transacciones_dlq") if t in existentes]
    if borrar:
        for f in adm.delete_topics(borrar).values():
            f.result()
        esperar(lambda: not (set(adm.list_topics(timeout=10).topics) & set(borrar)), 30, 1, "borrado de topics")
    for _ in range(20):
        try:
            for f in adm.create_topics([NewTopic("transacciones", 12, 1), NewTopic("transacciones_dlq", 3, 1)]).values():
                f.result()
            break
        except Exception:  # noqa: BLE001
            time.sleep(2)
    d = boto3.client("dynamodb", region_name="us-east-1", endpoint_url=ENV["DYNAMODB_ENDPOINT"])
    try:
        d.delete_table(TableName="alertas_fraude")
    except Exception:  # noqa: BLE001
        pass
    py("infra.aws.crear_recursos", "--solo", "dynamodb")


def iniciar_streaming(log_path):
    return subprocess.Popen([sys.executable, "-m", "streaming.job"], cwd=RAIZ, env=ENV,
                            stdout=open(log_path, "ab"), stderr=subprocess.STDOUT, start_new_session=True)


def producir(*args):
    r = py("producer.producer", "--verdad", VERDAD, "--clientes", "2000", "--comercios", "100", *args)
    if r.returncode:
        print(r.stdout[-1500:], r.stderr[-1500:])
    return r


def main():
    global VERDAD
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake", default="/tmp/fl-lake")
    a = ap.parse_args()
    lake = a.lake
    ENV["LAKE_URI"] = lake
    VERDAD = os.path.join(lake + "-verdad.jsonl")
    shutil.rmtree(lake, ignore_errors=True)
    if os.path.exists(VERDAD):
        os.remove(VERDAD)

    log("0. Reinicio de Kafka (topics), DynamoDB y lakehouse local")
    reiniciar_infra()

    log("1. Catálogos: CSV -> landing -> Bronze -> Silver (modo CSV) y lista de riesgo v1")
    py("producer.catalogo", "--clientes", "2000", "--comercios", "100", "--csv-dir", os.path.join(lake, "landing", "catalogos_csv"))
    py("producer.catalogo", "--lista-riesgo", "1", "--destino", os.path.join(lake, "landing", "listas_riesgo"))
    cat = dict(CATALOGOS_SOURCE="csv")
    verificar("hay_csv_nuevo detecta la lista v1 (sensor)", py("batch.catalogos", "hay_csv_nuevo", **cat).returncode == 0)
    for paso in ("extract_batch", "load_bronze", "spark_silver"):
        r = py("batch.catalogos", paso, **cat)
        verificar("catalogos_diario/%s" % paso, r.returncode == 0)
    verificar("hay_csv_nuevo ya no detecta nada tras cargar", py("batch.catalogos", "hay_csv_nuevo", **cat).returncode == 1)
    cli = tabla(lake, "silver", "clientes")
    verificar("Silver clientes: 2.000 filas y sin datos personales en claro",
              len(cli) == 2000 and cli["documento_h"].str.len().eq(64).all() and "nombre" not in cli.columns)
    verificar("Silver lista_riesgo v1: 5 comercios + 5 dispositivos", len(tabla(lake, "silver", "lista_riesgo")) == 10)

    log("2. Streaming: arranque")
    slog = os.path.join(lake + "-streaming.log")
    job = iniciar_streaming(slog)
    verificar("el job escribe latido", esperar(lambda: os.path.exists(os.path.join(lake, "_estado", "streaming_latido.json")), 120, 2, "latido"))

    log("3. Historial para R2 (10 días por cliente) y tráfico normal")
    producir("--escenario", "r2-base")
    producir("--escenario", "normal", "--rate", "30", "--duracion", "12")
    esperar(lambda: len(tabla(lake, "silver", "transacciones")) >= 300, 120, 3, "Silver >= 300")

    log("4. Gold: dimensiones (perfil_cliente y dim_dispositivo alimentan a R2)")
    r = py("batch.gold", "dimensiones")
    verificar("gold dimensiones", r.returncode == 0)
    perfil = tabla(lake, "gold", "perfil_cliente")
    verificar("perfil_cliente tiene a los clientes con historial", len(perfil) > 0, "(%d clientes)" % len(perfil))

    log("5. Escenarios con respuesta conocida: R2, R3 (lista v1), R1, malformados y duplicados")
    producir("--escenario", "r2")
    producir("--escenario", "r3")
    producir("--escenario", "r1")
    producir("--escenario", "malformados")
    producir("--escenario", "duplicados")
    verdad = [json.loads(x) for x in open(VERDAD)]
    ids_r2 = {i for v in verdad if v["escenario"] == "r2" for i in v["ids"]}
    ids_r3 = [i for v in verdad if v["escenario"] == "r3" for i in v["ids"]]
    cli_r1 = [v["id_cliente"] for v in verdad if v["escenario"] == "r1"][0]
    esperar(lambda: len(por_regla(alertas()).get("R2", [])) >= 5 and len(por_regla(alertas()).get("R3", [])) >= 10
            and any(i["id_cliente"] == cli_r1 for i in por_regla(alertas()).get("R1", [])), 150, 3, "alertas R1/R2/R3")
    pr = por_regla(alertas())
    r2_tx = {i["id_transaccion"] for i in pr.get("R2", [])}
    verificar("R2: alerta exactamente en las 5 transacciones inyectadas", r2_tx == ids_r2, "(%d/5)" % len(r2_tx & ids_r2))
    r3_tx = {i["id_transaccion"] for i in pr.get("R3", [])}
    verificar("R3 con lista v1: 10 alertas (5 comercios + 5 dispositivos de la v1)", len(r3_tx) == 10 and r3_tx <= set(ids_r3), "(%d)" % len(r3_tx))
    verificar("R1: hay alertas del cliente de la ráfaga", any(i["id_cliente"] == cli_r1 for i in pr.get("R1", [])))
    falsos = {i["id_cliente"] for i in pr.get("R1", [])} - {cli_r1} - {v["id_cliente"] for v in verdad if "id_cliente" in v}
    verificar("R1: sin falsos positivos del tráfico normal", not falsos, str(sorted(falsos)[:3]))

    log("6. DLQ, cuarentena y duplicados")
    from confluent_kafka import Consumer

    c = Consumer({"bootstrap.servers": ENV["KAFKA_BOOTSTRAP_SERVERS"], "group.id": "verif-%d" % time.time(), "auto.offset.reset": "earliest"})
    c.subscribe(["transacciones_dlq"])
    dlq, fin = [], time.time() + 20
    while time.time() < fin and not dlq:
        m = c.poll(1.0)
        if m is not None and not m.error():
            dlq.append(m.value().decode())
    c.close()
    verificar("DLQ recibió el JSON ilegible", any("esto no es json" in x for x in dlq), "(%d msgs)" % len(dlq))
    esperar(lambda: len(tabla(lake, "silver", "cuarentena")) >= 4, 60, 3, "cuarentena")
    motivos = sorted(tabla(lake, "silver", "cuarentena")["motivo"])
    verificar("cuarentena con los 4 motivos", len(motivos) == 4, str(motivos))
    bronce, silver = tabla(lake, "bronze", "transacciones"), tabla(lake, "silver", "transacciones")
    verificar("Silver sin duplicados (los 3 reenviados no entraron)", silver["id_transaccion"].is_unique)
    verificar("Bronze conserva los reenvíos (admite duplicados)", len(bronce) > bronce["id_transaccion"].nunique())
    verificar("Silver sin datos personales en claro", silver["documento_h"].str.len().eq(64).all())

    log("7. Tolerancia a fallos: kill -9 al job, reinicio desde el checkpoint, sin duplicados")
    antes_silver, antes_alertas = len(tabla(lake, "silver", "transacciones")), {i["id_alerta"] for i in alertas()}
    os.killpg(job.pid, signal.SIGKILL)
    job.wait()
    producir("--escenario", "normal", "--rate", "20", "--duracion", "6")  # llega tráfico con el job caído (queda en Kafka)
    t_reinicio = time.time()
    job = iniciar_streaming(slog)
    ok = esperar(lambda: len(tabla(lake, "silver", "transacciones")) > antes_silver, 150, 3, "reanuda tras el reinicio")
    verificar("el job retoma desde el checkpoint y procesa lo acumulado", ok, "(%.0f s)" % (time.time() - t_reinicio))
    time.sleep(10)
    silver = tabla(lake, "silver", "transacciones")
    verificar("Silver sigue sin duplicados tras el reinicio", silver["id_transaccion"].is_unique)
    ahora_alertas = alertas()
    verificar("las alertas anteriores no cambian ni se duplican", antes_alertas <= {i["id_alerta"] for i in ahora_alertas}
              and len(ahora_alertas) == len({i["id_alerta"] for i in ahora_alertas}))
    sil_al = tabla(lake, "silver", "alertas")
    verificar("alertas en Silver sin duplicados", sil_al["id_alerta"].is_unique and set(sil_al["id_alerta"]) == {i["id_alerta"] for i in ahora_alertas})

    log("8. Lista de riesgo v2 (RF-03) y R3 con la nueva versión")
    py("producer.catalogo", "--lista-riesgo", "2", "--destino", os.path.join(lake, "landing", "listas_riesgo"))
    verificar("sensor detecta la v2", py("batch.catalogos", "hay_csv_nuevo", **cat).returncode == 0)
    for paso in ("load_bronze", "spark_silver"):
        py("batch.catalogos", paso, **cat)
    verificar("Silver tiene ambas versiones", set(tabla(lake, "silver", "lista_riesgo")["version"]) == {1, 2})
    n_r3_antes = len(por_regla(alertas()).get("R3", []))
    producir("--escenario", "r3")
    esperar(lambda: len(por_regla(alertas()).get("R3", [])) >= n_r3_antes + 20, 120, 3, "R3 con v2")
    nuevas = len(por_regla(alertas()).get("R3", [])) - n_r3_antes
    verificar("R3 con lista v2: las 20 transacciones (10 comercios + 10 dispositivos) disparan", nuevas == 20, "(%d)" % nuevas)

    log("9. Gold: dimensiones y hechos, calidad, consultas")
    verificar("gold dimensiones", py("batch.gold", "dimensiones").returncode == 0)
    verificar("gold hechos", py("batch.gold", "hechos").returncode == 0)
    verificar("gold hechos es idempotente (segunda corrida)", py("batch.gold", "hechos").returncode == 0)
    ft = tabla(lake, "gold", "fact_transacciones")
    fa = tabla(lake, "gold", "fact_alertas")
    silver = tabla(lake, "silver", "transacciones")
    verificar("fact_transacciones concilia con Silver y sin duplicados", len(ft) == len(silver) and ft["id_transaccion"].is_unique,
              "(gold=%d silver=%d)" % (len(ft), len(silver)))
    verificar("fact_alertas conserva latencia y sin duplicados", fa["latencia_s"].notna().all() and not fa.duplicated(["id_alerta", "rule_version"]).any())
    verificar("transacciones sospechosas marcadas (R1 por ventana, R2/R3 por transacción)", ft["es_sospechosa"].sum() >= 30,
              "(%d)" % ft["es_sospechosa"].sum())
    dc = tabla(lake, "gold", "dim_cliente")
    verificar("dim_cliente SCD2 con vigencia", dc["es_actual"].all() and (dc["valid_to"].astype(str) == "9999-12-31").all())
    r = py("batch.calidad", _ok=1)
    print(r.stdout[-600:] + r.stderr[-1500:])
    log("   quality_checks terminó con código %d (1 = detectó un problema; se revisa abajo)" % r.returncode)
    r = py("batch.consultas", "todas")
    verificar("consultas P4, P5 y P6 corren sobre Gold", r.returncode == 0)
    print(r.stdout[-2500:])
    py("batch.publicar", "optimize")

    log("10. Reproceso con v2 (RF-16): no toca Bronze ni Silver")
    b0, s0 = len(tabla(lake, "bronze", "transacciones")), len(tabla(lake, "silver", "transacciones"))
    hoy = time.strftime("%Y-%m-%d", time.gmtime())
    r = py("batch.reproceso", "--desde", hoy, "--hasta", hoy, "--rule-version", "v2")
    verificar("reproceso v2", r.returncode == 0)
    print(r.stdout[-1500:])
    fa = tabla(lake, "gold", "fact_alertas")
    verificar("Gold tiene alertas de reproceso con v2 y de streaming con v1",
              ((fa["rule_version"] == "v2") & (fa["origen"] == "reproceso")).any() and ((fa["rule_version"] == "v1") & (fa["origen"] == "streaming")).any())
    verificar("Bronze y Silver no cambiaron", len(tabla(lake, "bronze", "transacciones")) == b0 and len(tabla(lake, "silver", "transacciones")) == s0)

    os.killpg(job.pid, signal.SIGTERM)
    log("RESULTADO: %s" % ("TODO OK" if not FALLOS else "FALLARON: %s" % FALLOS))
    return 1 if FALLOS else 0


if __name__ == "__main__":
    sys.exit(main())

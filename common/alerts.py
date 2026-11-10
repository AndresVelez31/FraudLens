"""Escritura idempotente de alertas en DynamoDB y Silver, y notificación por SNS (RF-09, RNF-03).

Orden por lote (si el job muere en cualquier punto y el lote se repite, nada se duplica):
  1. DynamoDB: PutItem condicional con `attribute_not_exists`. Si la alerta ya existe (reintento o ventana de R1
     que se actualiza) se refrescan solo sus campos variables y se conservan ts_evento y ts_alerta originales.
  2. SNS: solo para las alertas cuya escritura condicional SÍ tuvo éxito (alertas realmente nuevas). No se manda un
     correo por alerta: se acumulan y sale un resumen, como máximo uno cada NOTIFICAR_CADA_SEGUNDOS (ver notificar).
  3. Silver: MERGE por id_alerta.
Compromiso conocido: si el job muere entre 1 y 2, esa alerta queda en DynamoDB pero no se notifica (al repetir el
lote ya existe). Se prefiere no enviar correos duplicados; la alerta sigue consultable en la API.
"""
import json
import logging
import threading
import time
from decimal import Decimal

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import config, tables

log = logging.getLogger("fraudlens.alertas")
FMT_TS = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"


def _tabla_dynamodb():
    import boto3

    return boto3.resource("dynamodb", region_name=config.REGION, endpoint_url=config.DYNAMODB_ENDPOINT).Table(
        config.DYNAMODB_TABLE
    )


def _a_dynamo(valor):
    return Decimal(str(valor)) if isinstance(valor, float) else valor


def a_filas(alertas_df, ts_alerta):
    """DataFrame de alertas -> lista de dicts con fechas ISO (texto, ordenable) listos para DynamoDB."""
    d = alertas_df.withColumn("ts_alerta", F.lit(ts_alerta))
    for c in ("ts_evento", "ts_alerta", "ventana_inicio", "ventana_fin"):
        d = d.withColumn(c, F.date_format(c, FMT_TS))
    return [{k: v for k, v in r.asDict().items() if v is not None} for r in d.collect()]


def escribir_dynamodb(filas):
    """Devuelve las alertas nuevas (las que no existían)."""
    from botocore.exceptions import ClientError

    tabla = _tabla_dynamodb()
    nuevas = []
    for a in filas:
        item = {k: _a_dynamo(v) for k, v in a.items()}
        try:
            tabla.put_item(Item=item, ConditionExpression="attribute_not_exists(id_alerta)")
            nuevas.append(a)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            # Ya existía: solo cambian los acumulados de R1 (n_tx, monto_total). No se toca ts_alerta.
            if a["regla"] == "R1":
                tabla.update_item(
                    Key={"id_alerta": a["id_alerta"]},
                    UpdateExpression="SET n_tx = :n, monto_total = :m",
                    ExpressionAttributeValues={":n": _a_dynamo(a.get("n_tx")), ":m": _a_dynamo(a.get("monto_total"))},
                )
    return nuevas


# --------------------------------------------------------------------------------------------------
# Notificación: un correo resumen, como máximo uno cada NOTIFICAR_CADA_SEGUNDOS
# --------------------------------------------------------------------------------------------------
# Con un correo por alerta, una ráfaga de tráfico llena el buzón del analista (una prueba de 20.000 pagos generó más
# de 500 correos). Las alertas nuevas se acumulan y se envía un solo resumen con el conteo por regla y las de mayor
# riesgo. El primero sale de inmediato; los siguientes esperan al menos NOTIFICAR_CADA_SEGUNDOS. Lo pendiente lo envía
# `enviar_pendientes()`, que el job llama en cada latido. Las dos consultas del streaming (ingesta y R1) corren en
# hilos distintos del driver: el estado se protege con un candado.
MAX_DETALLE = 100           # alertas que se listan en el correo (~13 KB; el límite de un mensaje de SNS es 256 KB)
_MAX_GUARDADAS = MAX_DETALLE  # se guardan solo las de mayor riesgo; el resto solo se cuenta
_candado = threading.Lock()
_pendiente = {"conteo": {}, "alertas": [], "primera": None}
_ultimo_envio = [None]


def _cada_segundos():
    return float(config.env("NOTIFICAR_CADA_SEGUNDOS", "300"))


def _riesgo(a):
    """Orden del correo: primero R4 por puntaje; luego las demás por monto."""
    puntaje = None
    if a.get("regla") == "R4":
        try:
            puntaje = json.loads(a.get("detalle") or "{}").get("puntaje")
        except ValueError:
            puntaje = None
    return (0, -(puntaje or 0)) if puntaje is not None else (1, -float(a.get("monto_total") or 0))


def resumen(conteo, alertas, desde=None):
    """(asunto, cuerpo) del correo para `conteo` {regla: n} y las alertas de mayor riesgo."""
    total = sum(conteo.values())
    por_regla = ", ".join("%s: %d" % (r, conteo[r]) for r in sorted(conteo))
    asunto = ("FraudLens: %d alerta%s nueva%s (%s)" % (total, "" if total == 1 else "s", "" if total == 1 else "s",
                                                      por_regla))[:100]
    lineas = ["FraudLens detectó %d alerta%s nueva%s%s." % (total, "" if total == 1 else "s", "" if total == 1 else "s",
                                                           " desde %s" % desde if desde else ""), "",
              "Por regla: " + por_regla, "",
              ("Todas, de mayor a menor riesgo:" if total <= MAX_DETALLE else
               "Las %d de mayor riesgo (de %d):" % (min(MAX_DETALLE, len(alertas)), total)),
              "regla | cliente | transacción | tipo | monto | hora del pago | puntaje"]
    for a in sorted(alertas, key=_riesgo)[:MAX_DETALLE]:
        extra = ""
        if a.get("regla") == "R4":
            try:
                extra = " | %.2f" % json.loads(a.get("detalle") or "{}")["puntaje"]
            except (ValueError, KeyError, TypeError):
                extra = ""
        lineas.append("- %s | %s | %s | %s | %s | %s%s" % (
            a.get("regla"), a.get("id_cliente"), a.get("id_transaccion") or "(ventana de 5 min)",
            a.get("id_comercio") or "-", a.get("monto_total"), a.get("ts_evento"), extra or " | -"))
    if total > MAX_DETALLE:
        lineas.append("... y %d más." % (total - MAX_DETALLE))
    filtro = "&desde=%s" % desde if desde else ""
    lineas += ["", "Cada alerta tiene todos sus campos en la tabla %s de DynamoDB y en la API:" % config.DYNAMODB_TABLE,
               "  todas las de este correo:   GET /alertas?limite=500%s" % filtro,
               "  solo las del modelo (R4):   GET /alertas?regla=R4%s" % filtro,
               "  las de un cliente:          GET /alertas?id_cliente=<cliente>"]
    return asunto, "\n".join(lineas)


def _enviar(asunto, cuerpo):
    if config.SNS_TOPIC_ARN:
        import boto3

        boto3.client("sns", region_name=config.REGION).publish(TopicArn=config.SNS_TOPIC_ARN, Subject=asunto, Message=cuerpo)
    else:
        log.info("RESUMEN DE ALERTAS (sin SNS configurado): %s", asunto)


def enviar_pendientes(forzar=False, ahora=None):
    """Envía el resumen acumulado si ya pasó el intervalo (o si `forzar`). Devuelve cuántas alertas cubrió."""
    ahora = ahora if ahora is not None else time.monotonic()
    with _candado:
        n = sum(_pendiente["conteo"].values())
        if not n:
            return 0
        if not forzar and _ultimo_envio[0] is not None and ahora - _ultimo_envio[0] < _cada_segundos():
            return 0
        asunto, cuerpo = resumen(_pendiente["conteo"], _pendiente["alertas"], _pendiente["primera"])
        _pendiente.update(conteo={}, alertas=[], primera=None)
        _ultimo_envio[0] = ahora
    try:
        _enviar(asunto, cuerpo)
    except Exception:  # noqa: BLE001  (un fallo de SNS no tumba el lote: las alertas siguen en DynamoDB)
        log.exception("No se pudo enviar el resumen de %d alertas por SNS", n)
    return n


def notificar(nuevas, ahora=None):
    """Acumula las alertas nuevas y envía el resumen si ya toca. Devuelve cuántas alertas cubrió el envío."""
    if not nuevas:
        return 0
    with _candado:
        for a in nuevas:
            _pendiente["conteo"][a["regla"]] = _pendiente["conteo"].get(a["regla"], 0) + 1
        _pendiente["primera"] = _pendiente["primera"] or nuevas[0].get("ts_alerta")
        _pendiente["alertas"] = sorted(_pendiente["alertas"] + list(nuevas), key=_riesgo)[:_MAX_GUARDADAS]
    return enviar_pendientes(ahora=ahora)


def merge_silver(spark, alertas_df, ts_alerta, batch_id):
    """Inserta la alerta nueva o actualiza los acumulados de una ventana de R1 ya registrada."""
    df = (
        alertas_df.withColumn("ts_alerta", F.lit(ts_alerta))
        .withColumn("batch_id", F.lit(str(batch_id)))
        .withColumn("source_system", F.lit(config.SOURCE_SYSTEM))
        .withColumn("ingest_ts", F.current_timestamp())
        .select(
            "id_alerta", "regla", "rule_version", "id_transaccion", "id_cliente", "id_comercio", "ts_evento",
            "ts_alerta", "ventana_inicio", "ventana_fin", "n_tx", "monto_total", "detalle", "kafka_partition",
            "kafka_offset", "batch_id", "source_system", "ingest_ts",
        )
        .dropDuplicates(["id_alerta"])
    )
    (
        DeltaTable.forPath(spark, tables.ruta("silver_alertas"))
        .alias("t")
        .merge(df.alias("s"), "t.id_alerta = s.id_alerta")
        .whenMatchedUpdate(set={"n_tx": "s.n_tx", "monto_total": "s.monto_total", "detalle": "s.detalle"})
        .whenNotMatchedInsertAll()
        .execute()
    )


def publicar(spark, alertas_df, batch_id):
    """Punto único de salida de alertas de un micro-batch. Devuelve (total, nuevas)."""
    from datetime import datetime, timezone

    if alertas_df.rdd.isEmpty():
        return 0, 0
    ahora = datetime.now(timezone.utc)
    alertas_df = alertas_df.dropDuplicates(["id_alerta"]).cache()
    filas = a_filas(alertas_df, ahora)
    nuevas = escribir_dynamodb(filas)
    notificar(nuevas)
    merge_silver(spark, alertas_df, ahora, batch_id)
    alertas_df.unpersist()
    return len(filas), len(nuevas)

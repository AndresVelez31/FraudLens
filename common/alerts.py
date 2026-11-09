"""Escritura idempotente de alertas en DynamoDB y Silver, y notificación por SNS (RF-09, RNF-03).

Orden por lote (si el job muere en cualquier punto y el lote se repite, nada se duplica):
  1. DynamoDB: PutItem condicional con `attribute_not_exists`. Si la alerta ya existe (reintento o ventana de R1
     que se actualiza) se refrescan solo sus campos variables y se conservan ts_evento y ts_alerta originales.
  2. SNS: solo para las alertas cuya escritura condicional SÍ tuvo éxito (alertas realmente nuevas).
  3. Silver: MERGE por id_alerta.
Compromiso conocido: si el job muere entre 1 y 2, esa alerta queda en DynamoDB pero no se notifica (al repetir el
lote ya existe). Se prefiere no enviar correos duplicados; la alerta sigue consultable en la API.
"""
import json
import logging
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


def notificar(nuevas):
    """SNS para alertas nuevas. Un fallo de SNS no tumba el lote: se registra y la alerta sigue en DynamoDB."""
    if not nuevas:
        return 0
    cliente = None
    if config.SNS_TOPIC_ARN:
        import boto3

        cliente = boto3.client("sns", region_name=config.REGION)
    enviadas = 0
    for a in nuevas:
        mensaje = json.dumps(a, default=str, ensure_ascii=False)
        asunto = "FraudLens %s cliente %s" % (a["regla"], a["id_cliente"])
        try:
            if cliente:
                cliente.publish(TopicArn=config.SNS_TOPIC_ARN, Subject=asunto[:100], Message=mensaje)
            else:
                log.info("ALERTA NUEVA (sin SNS configurado): %s %s", asunto, mensaje)
            enviadas += 1
        except Exception:  # noqa: BLE001
            log.exception("No se pudo notificar la alerta %s por SNS", a["id_alerta"])
    return enviadas


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

"""Reproceso de un rango de fechas con otra versión de las reglas (RF-16, RNF-06).

Recalcula R1-R3 desde Silver y escribe el resultado en Gold (`fact_alertas`, con su `rule_version` y origen
'reproceso'). NO modifica Bronze ni Silver, ni DynamoDB (el reproceso es análisis histórico, no genera notificaciones).

  python -m batch.reproceso --desde 2026-11-04 --hasta 2026-11-04 --rule-version v2

Al terminar imprime, por regla, cuántas alertas dio cada versión para poder compararlas.
R2 se calcula "as-of": un dispositivo es nuevo si esa fue la primera vez que el cliente lo usó, y el promedio es el
de los 30 días anteriores a cada transacción (por eso se leen 30 días de historia antes del rango).
"""
import argparse
import logging
import sys
import uuid
from datetime import date, timedelta

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import config, rules, tables
from common.spark import get_spark

log = logging.getLogger("fraudlens.reproceso")


def calcular(spark, desde, hasta, version):
    """Alertas de la versión `version` para transacciones entre `desde` y `hasta` (ambas inclusive)."""
    c = rules.config_version(version)
    historia_desde = (date.fromisoformat(desde) - timedelta(days=c["r2_dias"])).isoformat()
    tx = spark.read.format("delta").load(tables.ruta("silver_transacciones"))
    historia = tx.where((F.col("fecha") >= historia_desde) & (F.col("fecha") <= hasta))
    rango = historia.where(F.col("fecha") >= desde)

    r1 = rules.r1_alertas(rules.r1_agregar(rango), version)
    r2 = rules.r2_alertas_historico(historia, version).where(F.col("ts_evento") >= F.lit(desde).cast("timestamp"))
    partes = [r1, r2]
    ruta_lista = config.lake("silver", "lista_riesgo")
    if DeltaTable.isDeltaTable(spark, ruta_lista):
        lista = rules.lista_vigente(spark.read.format("delta").load(ruta_lista))
        partes.append(rules.r3_alertas(rango, lista, version))
    else:
        log.warning("sin lista de riesgo cargada: R3 omitida")
    alertas = partes[0]
    for p in partes[1:]:
        alertas = alertas.unionByName(p)
    return alertas


def escribir_gold(spark, alertas, batch_id):
    df = alertas.select(
        "id_alerta", "rule_version", F.col("regla").alias("id_regla"), "id_transaccion", "id_cliente", "id_comercio",
        "ts_evento", F.current_timestamp().alias("ts_alerta"), "ventana_inicio", "ventana_fin", "n_tx", "monto_total",
        F.lit(None).cast("double").alias("latencia_s"), F.to_date("ts_evento").alias("fecha"),
        F.lit("reproceso").alias("origen"), F.lit(config.SOURCE_SYSTEM).alias("source_system"),
        "kafka_partition", "kafka_offset", F.lit(batch_id).alias("batch_id"), F.current_timestamp().alias("ingest_ts"),
    ).dropDuplicates(["id_alerta", "rule_version"])
    ruta = config.lake("gold", "fact_alertas")
    if not DeltaTable.isDeltaTable(spark, ruta):
        df.write.format("delta").mode("overwrite").partitionBy("fecha").save(ruta)
        return
    (
        DeltaTable.forPath(spark, ruta).alias("t")
        .merge(df.alias("s"), "t.id_alerta = s.id_alerta AND t.rule_version = s.rule_version AND t.origen = s.origen")
        .whenMatchedUpdate(set={"n_tx": "s.n_tx", "monto_total": "s.monto_total", "ventana_fin": "s.ventana_fin"})
        .whenNotMatchedInsertAll().execute()
    )


def comparar(spark, desde, hasta):
    gold = spark.read.format("delta").load(config.lake("gold", "fact_alertas")).where(
        (F.col("fecha") >= desde) & (F.col("fecha") <= hasta))
    return gold.groupBy("id_regla").pivot("rule_version").count().orderBy("id_regla"), \
        gold.groupBy("rule_version", "origen").count().orderBy("rule_version", "origen")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde", required=True, help="YYYY-MM-DD")
    ap.add_argument("--hasta", required=True, help="YYYY-MM-DD")
    ap.add_argument("--rule-version", required=True, choices=sorted(rules.RULES))
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = get_spark("fraudlens-reproceso")
    try:
        batch_id = "repro-%s" % uuid.uuid4().hex[:8]
        alertas = calcular(spark, a.desde, a.hasta, a.rule_version).cache()
        log.info("%d alertas con %s entre %s y %s", alertas.count(), a.rule_version, a.desde, a.hasta)
        escribir_gold(spark, alertas, batch_id)
        por_regla, por_origen = comparar(spark, a.desde, a.hasta)
        print("\nAlertas por regla y versión (Gold):")
        por_regla.show(truncate=False)
        por_origen.show(truncate=False)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

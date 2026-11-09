"""Tarea `quality_checks` del DAG `fraude_pipeline` (RNF-08, RNF-03): si algo está mal, sale con error y el DAG se detiene.

Controles:
  1. Tasa de rechazo (JSON ilegible + cuarentena) de la última ventana <= 1 % (si hay al menos MIN_FILAS eventos).
  2. Silver sin transacciones duplicadas (exactly-once efectivo).
  3. Silver sin alertas duplicadas.
  4. Gold sin transacciones duplicadas y sin llaves nulas.
  5. Conciliación: Gold tiene todas las transacciones de Silver de los últimos días.
"""
import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

from common import config, tables
from common.spark import get_spark

log = logging.getLogger("fraudlens.calidad")
UMBRAL_RECHAZO = float(config.env("UMBRAL_RECHAZO", "0.01"))
MIN_FILAS = int(config.env("CALIDAD_MIN_FILAS", "100"))


def leer(spark, ruta):
    return spark.read.format("delta").load(ruta)


def verificar(spark, ventana_min=60, dias=2):
    """Devuelve la lista de problemas encontrados (vacía si todo está bien) y las métricas."""
    problemas, m = [], {}
    corte = datetime.now(timezone.utc) - timedelta(minutes=ventana_min)
    f0 = (datetime.now(timezone.utc) - timedelta(days=dias)).date().isoformat()

    bronce = leer(spark, tables.ruta("bronze_transacciones")).where(F.col("ingest_ts") >= F.lit(corte))
    cuar = leer(spark, tables.ruta("silver_cuarentena")).where(F.col("ingest_ts") >= F.lit(corte))
    n_bronce = bronce.count()
    ilegibles = bronce.where(F.get_json_object("value", "$").isNull()).count()  # texto que no es JSON
    n_cuar = cuar.count()
    m.update(bronze_ventana=n_bronce, ilegibles=ilegibles, cuarentena=n_cuar)
    if n_bronce >= MIN_FILAS:
        tasa = (ilegibles + n_cuar) / n_bronce
        m["tasa_rechazo"] = round(tasa, 4)
        if tasa > UMBRAL_RECHAZO:
            problemas.append("tasa de rechazo %.2f%% supera el %.1f%%" % (100 * tasa, 100 * UMBRAL_RECHAZO))

    silver = leer(spark, tables.ruta("silver_transacciones")).where(F.col("fecha") >= f0)
    n_silver = silver.count()
    dup_silver = n_silver - silver.select("id_transaccion").distinct().count()
    m.update(silver_recientes=n_silver, duplicados_silver=dup_silver)
    if dup_silver:
        problemas.append("Silver tiene %d transacciones duplicadas" % dup_silver)

    al = leer(spark, tables.ruta("silver_alertas"))
    dup_al = al.count() - al.select("id_alerta").distinct().count()
    m["duplicados_alertas"] = dup_al
    if dup_al:
        problemas.append("Silver tiene %d alertas duplicadas" % dup_al)

    gold = leer(spark, config.lake("gold", "fact_transacciones")).where(F.col("fecha") >= f0)
    n_gold = gold.count()
    dup_gold = n_gold - gold.select("id_transaccion").distinct().count()
    nulos = gold.where(F.col("id_cliente").isNull() | F.col("id_comercio").isNull() | F.col("id_tiempo").isNull()).count()
    m.update(gold_recientes=n_gold, duplicados_gold=dup_gold, llaves_nulas_gold=nulos)
    if dup_gold:
        problemas.append("Gold tiene %d transacciones duplicadas" % dup_gold)
    if nulos:
        problemas.append("Gold tiene %d filas con llaves nulas" % nulos)
    # Silver puede ir unos segundos por delante de Gold (el DAG corre cada 10 min): se concilia contra lo ya cargado.
    faltan = silver.join(gold.select("id_transaccion"), "id_transaccion", "left_anti").where(
        F.col("ingest_ts") < F.lit(datetime.now(timezone.utc) - timedelta(minutes=15))).count()
    m["silver_sin_gold"] = faltan
    if faltan:
        problemas.append("%d transacciones de Silver (con más de 15 min) no llegaron a Gold" % faltan)
    return problemas, m


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ventana-min", type=int, default=int(config.env("QUALITY_VENTANA_MIN", "60")))
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = get_spark("fraudlens-calidad")
    try:
        problemas, m = verificar(spark, a.ventana_min)
    finally:
        spark.stop()
    log.info("métricas de calidad: %s", m)
    if problemas:
        for p in problemas:
            log.error("CALIDAD FALLÓ: %s", p)
        return 1
    log.info("quality_checks OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())

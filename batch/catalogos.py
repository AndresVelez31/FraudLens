"""DAG `catalogos_diario` (RF-02, RF-03): PostgreSQL y CSV de riesgo -> landing -> Bronze -> Silver.

Cada paso es un subcomando y se puede repetir sin duplicar nada (idempotente):

  python -m batch.catalogos extract_batch   PostgreSQL (JDBC) o CSV  -> landing/catalogos/<tabla>/snapshot_fecha=...
  python -m batch.catalogos load_bronze     landing -> Bronze (replaceWhere por snapshot) + listas de riesgo nuevas
  python -m batch.catalogos spark_silver    Bronze -> Silver (último snapshot, datos personales con HMAC)
  python -m batch.catalogos hay_csv_nuevo   código de salida 0 si hay una versión de lista de riesgo sin cargar (sensor)
"""
import argparse
import logging
import re
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

from common import config, lake, pii
from common.spark import get_spark

log = logging.getLogger("fraudlens.catalogos")
TABLAS = ["clientes", "comercios"]


def hoy():
    return datetime.now(timezone.utc).date().isoformat()


def leer_fuente(spark, tabla):
    if config.env("CATALOGOS_SOURCE", "jdbc") == "csv":  # modo sin PostgreSQL (pruebas)
        return spark.read.option("header", True).csv(config.lake("landing", "catalogos_csv", "%s.csv" % tabla))
    url = "jdbc:postgresql://%s:%s/%s" % (
        config.env("POSTGRES_HOST", "postgres"), config.env("POSTGRES_PORT", "5432"), config.env("POSTGRES_DB", "fraudlens"))
    return (
        spark.read.format("jdbc").option("url", url).option("dbtable", tabla)
        .option("user", config.env("POSTGRES_USER", "fraudlens")).option("password", config.env("POSTGRES_PASSWORD", ""))
        .option("driver", "org.postgresql.Driver").load()
    )


def extract_batch(spark, fecha):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    for t in TABLAS:
        df = leer_fuente(spark, t).withColumn("snapshot_fecha", F.lit(fecha))
        df.write.mode("overwrite").partitionBy("snapshot_fecha").parquet(config.lake("landing", "catalogos", t))
        log.info("landing/catalogos/%s: %d filas (snapshot %s)", t, df.count(), fecha)


def _auditoria(df, batch_id, origen):
    return (
        df.withColumn("batch_id", F.lit(batch_id)).withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_system", F.lit(origen))
    )


def versiones_cargadas(spark):
    from delta.tables import DeltaTable

    ruta = config.lake("bronze", "lista_riesgo")
    if not DeltaTable.isDeltaTable(spark, ruta):
        return set()
    return {r["version"] for r in spark.read.format("delta").load(ruta).select("version").distinct().collect()}


def csv_pendientes(spark):
    """[(version, uri)] de los CSV en landing/listas_riesgo/ que aún no están en Bronze."""
    ya = versiones_cargadas(spark)
    pendientes = []
    for uri in lake.listar(config.lake("landing", "listas_riesgo")):
        m = re.search(r"riesgo_v(\d+)\.csv$", uri)
        if m and int(m.group(1)) not in ya:
            pendientes.append((int(m.group(1)), uri))
    return sorted(pendientes)


def load_bronze(spark, fecha):
    batch_id = "cat-%s" % uuid.uuid4().hex[:8]
    for t in TABLAS:
        df = spark.read.parquet(config.lake("landing", "catalogos", t)).where(F.col("snapshot_fecha") == fecha)
        _auditoria(df, batch_id, "postgresql").write.format("delta").mode("overwrite").option(
            "replaceWhere", "snapshot_fecha = '%s'" % fecha
        ).partitionBy("snapshot_fecha").save(config.lake("bronze", "catalogos_%s" % t))
        log.info("bronze/catalogos_%s: snapshot %s cargado", t, fecha)
    for version, uri in csv_pendientes(spark):
        df = (
            spark.read.option("header", True).csv(uri)
            .withColumn("version", F.lit(version)).withColumn("archivo", F.lit(uri.rsplit("/", 1)[-1]))
        )
        _auditoria(df, batch_id, "csv").write.format("delta").mode("append").save(config.lake("bronze", "lista_riesgo"))
        log.info("bronze/lista_riesgo: versión %d cargada (%s)", version, uri)


def spark_silver(spark):
    def ultimo_snapshot(tabla):
        b = spark.read.format("delta").load(config.lake("bronze", "catalogos_%s" % tabla))
        return b.where(F.col("snapshot_fecha") == b.agg(F.max("snapshot_fecha")).collect()[0][0])

    cli = ultimo_snapshot("clientes").select(
        "id_cliente", "ciudad", "segmento", F.to_date("fecha_alta").alias("fecha_alta"),
        pii.hmac_col(F.col("documento")).alias("documento_h"),
        pii.hmac_col(F.col("telefono")).alias("telefono_h"),
        pii.hmac_col(F.col("correo")).alias("correo_h"),  # el nombre no se conserva: no hace falta para el análisis
        F.col("snapshot_fecha"),
    )
    cli.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(config.lake("silver", "clientes"))
    com = ultimo_snapshot("comercios").select("id_comercio", "nombre", "ciudad", "categoria", "snapshot_fecha")
    com.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(config.lake("silver", "comercios"))

    ruta_lista = config.lake("bronze", "lista_riesgo")
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, ruta_lista):
        lista = (
            spark.read.format("delta").load(ruta_lista)
            .select("version", "tipo", "id_entidad", "motivo", F.col("ingest_ts").alias("cargado_ts"))
            .dropDuplicates(["version", "tipo", "id_entidad"])
        )
        lista.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(config.lake("silver", "lista_riesgo"))
        log.info("silver/lista_riesgo: versiones %s", sorted(r["version"] for r in lista.select("version").distinct().collect()))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paso", choices=["extract_batch", "load_bronze", "spark_silver", "hay_csv_nuevo"])
    ap.add_argument("--fecha", default=hoy())
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = get_spark("fraudlens-catalogos-" + a.paso)
    try:
        if a.paso == "hay_csv_nuevo":
            p = csv_pendientes(spark)
            print("pendientes:", p)
            return 0 if p else 1
        {"extract_batch": lambda: extract_batch(spark, a.fecha), "load_bronze": lambda: load_bronze(spark, a.fecha),
         "spark_silver": lambda: spark_silver(spark)}[a.paso]()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

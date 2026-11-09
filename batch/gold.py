"""DAG `fraude_pipeline` (RF-10, RF-11 a RF-13): Silver -> Gold en esquema en estrella.

  python -m batch.gold dimensiones   dim_cliente (SCD2), dim_comercio, dim_dispositivo, dim_tiempo, dim_regla, perfil_cliente
  python -m batch.gold hechos        fact_transacciones y fact_alertas (+ marca de transacciones sospechosas)

Todo es idempotente (MERGE o sobrescritura de una tabla derivada): correr dos veces el mismo paso deja el mismo Gold.
Los hechos se procesan de forma incremental sobre los últimos `--dias` días de Silver (partición `fecha`).
"""
import argparse
import logging
import sys
import uuid
from datetime import datetime, timedelta, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import config, rules, tables
from common.spark import get_spark

log = logging.getLogger("fraudlens.gold")
FIN_DE_LOS_TIEMPOS = "9999-12-31"


def ruta(nombre):
    return config.lake("gold", nombre)


def existe(spark, nombre):
    return DeltaTable.isDeltaTable(spark, ruta(nombre))


def escribir(spark, df, nombre, particion=None):
    """Sobrescribe una tabla derivada completa (idempotente) y fija el historial de 90 días."""
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if particion:
        w = w.partitionBy(particion)
    w.save(ruta(nombre))
    tables.fijar_retencion(spark, ruta(nombre))


def desde(dias):
    return (datetime.now(timezone.utc) - timedelta(days=dias)).date().isoformat()


# ---------------------------------------------------------------- dimensiones
def dim_cliente_scd2(spark):
    """SCD tipo 2: si cambian ciudad o segmento se cierra la versión vigente y se abre una nueva (conserva historial)."""
    src = spark.read.format("delta").load(config.lake("silver", "clientes")).select(
        "id_cliente", "ciudad", "segmento", "fecha_alta",
        F.sha2(F.concat_ws("|", "ciudad", "segmento"), 256).alias("hash_atributos"),
        F.to_date("snapshot_fecha").alias("valid_from"),
    )
    if not existe(spark, "dim_cliente"):
        nuevo = src.withColumn("valid_to", F.to_date(F.lit(FIN_DE_LOS_TIEMPOS))).withColumn("es_actual", F.lit(True))
        nuevo = nuevo.withColumn("sk_cliente", F.sha2(F.concat_ws("|", "id_cliente", "valid_from"), 256))
        escribir(spark, nuevo, "dim_cliente")
        return
    tgt = DeltaTable.forPath(spark, ruta("dim_cliente"))
    # 1) cierra las versiones vigentes cuyos atributos cambiaron
    (
        tgt.alias("t")
        .merge(src.alias("s"), "t.id_cliente = s.id_cliente AND t.es_actual = true AND t.hash_atributos <> s.hash_atributos")
        .whenMatchedUpdate(set={"valid_to": "s.valid_from", "es_actual": "false"})
        .execute()
    )
    # 2) abre versión nueva para los clientes que quedaron sin versión vigente (cambiaron o son nuevos)
    vigentes = spark.read.format("delta").load(ruta("dim_cliente")).where("es_actual = true").select("id_cliente")
    nuevos = (
        src.join(vigentes, "id_cliente", "left_anti")
        .withColumn("valid_to", F.to_date(F.lit(FIN_DE_LOS_TIEMPOS))).withColumn("es_actual", F.lit(True))
        .withColumn("sk_cliente", F.sha2(F.concat_ws("|", "id_cliente", "valid_from"), 256))
    )
    nuevos.write.format("delta").mode("append").save(ruta("dim_cliente"))


def dim_dispositivo(spark, dias):
    tx = spark.read.format("delta").load(tables.ruta("silver_transacciones"))
    if existe(spark, "dim_dispositivo"):
        tx = tx.where(F.col("fecha") >= desde(dias))
    agg = tx.groupBy("id_cliente", "id_dispositivo").agg(
        F.min("ts_evento").alias("primera_vez"), F.max("ts_evento").alias("ultima_vez")
    )
    if not existe(spark, "dim_dispositivo"):
        escribir(spark, agg, "dim_dispositivo")
        return
    (
        DeltaTable.forPath(spark, ruta("dim_dispositivo")).alias("t")
        .merge(agg.alias("s"), "t.id_cliente = s.id_cliente AND t.id_dispositivo = s.id_dispositivo")
        .whenMatchedUpdate(set={
            "primera_vez": "least(t.primera_vez, s.primera_vez)", "ultima_vez": "greatest(t.ultima_vez, s.ultima_vez)"})
        .whenNotMatchedInsertAll().execute()
    )


def perfil_cliente(spark):
    """Promedio de monto de los últimos 30 días por cliente: la referencia de R2."""
    tx = spark.read.format("delta").load(tables.ruta("silver_transacciones")).where(F.col("fecha") >= desde(30))
    perfil = tx.groupBy("id_cliente").agg(
        F.avg("monto").alias("avg_monto_30d"), F.count("*").alias("n_tx_30d")
    ).withColumn("actualizado_ts", F.current_timestamp())
    escribir(spark, perfil, "perfil_cliente")


def dim_tiempo(spark):
    limites = spark.read.format("delta").load(tables.ruta("silver_transacciones")).agg(
        F.min("fecha").alias("a"), F.max("fecha").alias("b")).collect()[0]
    if limites["a"] is None:
        return
    horas = spark.sql(
        "SELECT explode(sequence(to_timestamp('%s'), to_timestamp('%s') + INTERVAL 1 DAY, INTERVAL 1 HOUR)) AS ts"
        % (limites["a"], limites["b"])
    )
    h = F.hour("ts")
    dim = horas.select(
        F.date_format("ts", "yyyyMMddHH").cast("int").alias("id_tiempo"),
        F.to_date("ts").alias("fecha"), F.year("ts").alias("anio"), F.month("ts").alias("mes"), F.dayofmonth("ts").alias("dia"),
        h.alias("hora"),
        F.when(h < 6, "madrugada").when(h < 12, "manana").when(h < 18, "tarde").otherwise("noche").alias("franja"),
        F.dayofweek("ts").alias("dia_semana"), F.weekofyear("ts").alias("semana_iso"),
        (F.dayofweek("ts").isin(1, 7)).alias("es_fin_de_semana"),
    )
    if not existe(spark, "dim_tiempo"):
        escribir(spark, dim, "dim_tiempo")
        return
    (
        DeltaTable.forPath(spark, ruta("dim_tiempo")).alias("t")
        .merge(dim.alias("s"), "t.id_tiempo = s.id_tiempo").whenNotMatchedInsertAll().execute()
    )


def dimensiones(spark, dias):
    dim_cliente_scd2(spark)
    escribir(spark, spark.read.format("delta").load(config.lake("silver", "comercios")).drop("snapshot_fecha"), "dim_comercio")
    dim_dispositivo(spark, dias)
    perfil_cliente(spark)
    dim_tiempo(spark)
    escribir(
        spark, spark.createDataFrame(rules.DIM_REGLA, "id_regla string, nombre string, descripcion string")
        .withColumn("versiones", F.lit(",".join(sorted(rules.RULES)))), "dim_regla")
    log.info("dimensiones de Gold actualizadas")


# ---------------------------------------------------------------- hechos
def _merge_o_crear(spark, df, nombre, condicion, particion=None, set_si_coincide=None):
    if not existe(spark, nombre):
        escribir(spark, df, nombre, particion)
        return
    m = DeltaTable.forPath(spark, ruta(nombre)).alias("t").merge(df.alias("s"), condicion)
    if set_si_coincide:
        m = m.whenMatchedUpdate(set=set_si_coincide)
    m.whenNotMatchedInsertAll().execute()


def hechos(spark, dias):
    batch_id = "gold-%s" % uuid.uuid4().hex[:8]
    f0 = desde(dias) if existe(spark, "fact_transacciones") else "1970-01-01"  # la primera vez se toma todo Silver

    # fact_transacciones: una fila por transacción de Silver; conserva partición y offset de Kafka (RNF-06)
    tx = spark.read.format("delta").load(tables.ruta("silver_transacciones")).where(F.col("fecha") >= f0).select(
        "id_transaccion", "id_cliente", "id_comercio", "id_dispositivo",
        F.date_format("ts_evento", "yyyyMMddHH").cast("int").alias("id_tiempo"),
        "ts_evento", "fecha", "monto", "tipo", F.lit(False).alias("es_sospechosa"),
        "source_system", "kafka_partition", "kafka_offset", F.col("ingest_ts").alias("ingest_ts"),
        F.lit(batch_id).alias("batch_id"),
    )
    _merge_o_crear(spark, tx, "fact_transacciones", "t.id_transaccion = s.id_transaccion AND t.fecha = s.fecha", "fecha")

    # fact_alertas: una fila por (alerta, versión de reglas)
    al = spark.read.format("delta").load(tables.ruta("silver_alertas")).select(
        "id_alerta", "rule_version", F.col("regla").alias("id_regla"), "id_transaccion", "id_cliente", "id_comercio",
        "ts_evento", "ts_alerta", "ventana_inicio", "ventana_fin", "n_tx", "monto_total",
        (F.col("ts_alerta").cast("double") - F.col("ts_evento").cast("double")).alias("latencia_s"),
        F.to_date("ts_alerta").alias("fecha"),
        F.lit("streaming").alias("origen"), "source_system", "kafka_partition", "kafka_offset", F.lit(batch_id).alias("batch_id"),
        F.col("ingest_ts"),
    )
    _merge_o_crear(
        spark, al, "fact_alertas", "t.id_alerta = s.id_alerta AND t.rule_version = s.rule_version", "fecha",
        set_si_coincide={"n_tx": "s.n_tx", "monto_total": "s.monto_total", "ventana_fin": "s.ventana_fin"},
    )
    marcar_sospechosas(spark, f0)
    log.info("hechos de Gold actualizados (batch %s)", batch_id)


def marcar_sospechosas(spark, fecha_desde):
    """Una transacción es sospechosa si tiene alerta propia (R2, R3) o si cae dentro de la ventana de una R1."""
    alertas = spark.read.format("delta").load(ruta("fact_alertas")).where("origen = 'streaming'")
    tx = spark.read.format("delta").load(ruta("fact_transacciones")).where(F.col("fecha") >= fecha_desde)
    propias = alertas.where(F.col("id_transaccion").isNotNull()).select("id_transaccion")
    r1 = alertas.where("id_regla = 'R1'").select(
        F.col("id_cliente").alias("r_cli"), F.col("ventana_inicio").alias("v0"), F.col("ventana_fin").alias("v1"))
    en_ventana = (
        tx.select("id_transaccion", "id_cliente", "ts_evento")
        .join(r1, (F.col("id_cliente") == F.col("r_cli")) & (F.col("ts_evento") >= F.col("v0")) & (F.col("ts_evento") < F.col("v1")))
        .select("id_transaccion")
    )
    sospechosas = propias.union(en_ventana).distinct()
    (
        DeltaTable.forPath(spark, ruta("fact_transacciones")).alias("t")
        .merge(sospechosas.alias("s"), "t.id_transaccion = s.id_transaccion AND t.fecha >= '%s'" % fecha_desde)
        .whenMatchedUpdate(condition="t.es_sospechosa = false", set={"es_sospechosa": "true"})
        .execute()
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paso", choices=["dimensiones", "hechos"])
    ap.add_argument("--dias", type=int, default=int(config.env("GOLD_DIAS_INCREMENTAL", "2")),
                    help="días recientes de Silver que se reprocesan (la primera vez se toma todo)")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = get_spark("fraudlens-gold-" + a.paso)
    try:
        {"dimensiones": dimensiones, "hechos": hechos}[a.paso](spark, a.dias)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

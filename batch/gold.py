"""DAG `fraude_pipeline` (RF-10, RF-11 a RF-13): Silver -> Gold en esquema en estrella.

  python -m batch.gold dimensiones   dim_cliente (SCD2), dim_comercio, dim_dispositivo, dim_tiempo, dim_regla, perfil_cliente
  python -m batch.gold hechos        fact_transacciones y fact_alertas (+ marca de transacciones sospechosas)

Todo es idempotente (MERGE o sobrescritura de una tabla derivada): correr dos veces el mismo paso deja el mismo Gold.

Tres relojes distintos, cada uno para lo suyo (necesario para reproducir datos históricos como IEEE-CIS):
  * Incremental por LLEGADA (`ingest_ts`): cada corrida toma lo que entró a Silver en los últimos `--dias` días,
    sin importar la fecha del pago. Antes se filtraba por la fecha del evento y una transacción con fecha antigua
    que llegara hoy nunca pasaba a Gold.
  * Perfil de R2 con el reloj de los DATOS: el promedio de 30 días se calcula hacia atrás desde el último
    `ts_evento` de Silver, no desde la hora del servidor. En producción los dos coinciden; al reproducir un
    histórico, el reloj del servidor dejaría el perfil vacío.
  * Hora de negocio en Colombia (`ZONA_HORARIA`, por defecto America/Bogota): `id_tiempo`, hora y franja del día.
    Los timestamps se siguen guardando en UTC y la partición `fecha` sigue siendo la de Silver (UTC).
"""
import argparse
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import config, lake, rules, tables
from common.spark import get_spark

log = logging.getLogger("fraudlens.gold")
FIN_DE_LOS_TIEMPOS = "9999-12-31"
ZONA_HORARIA = config.env("ZONA_HORARIA", "America/Bogota")
PERFIL_DIAS = 30


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


# Marca de agua de Gold: la llegada (ingest_ts) más reciente que ya está en fact_transacciones. La fija `fijar_marca`
# al empezar cada paso; None = sin marca (la primera vez, o en las pruebas).
_MARCA = {"ts": None}
MARGEN_MARCA = timedelta(hours=1)   # cubre lotes del streaming que se confirman después de la lectura de Gold


def corte_ingesta(dias):
    """Instante desde el que se reprocesa: lo que llegó a Silver en los últimos `dias` días (reloj del servidor), pero
    nunca antes de la marca de agua de Gold menos un margen. Sin la marca, una carga grande (por ejemplo, los 590 mil
    pagos de IEEE-CIS en un día) se reprocesaba completa en cada corrida y el DAG pasaba su límite de 9 min."""
    corte = datetime.now(timezone.utc) - timedelta(days=dias)
    if _MARCA["ts"] is not None:
        corte = max(corte, _MARCA["ts"] - MARGEN_MARCA)
    return corte


def fijar_marca(spark):
    """Lee la marca de agua de Gold (max ingest_ts de fact_transacciones) para que el paso procese solo lo nuevo."""
    _MARCA["ts"] = None
    if existe(spark, "fact_transacciones"):
        m = spark.read.format("delta").load(ruta("fact_transacciones")).agg(F.max("ingest_ts")).first()[0]
        if m is not None:
            _MARCA["ts"] = m.replace(tzinfo=timezone.utc) if m.tzinfo is None else m
    log.info("marca de agua de Gold: %s", _MARCA["ts"])


def llegadas(df, dias):
    return df.where(F.col("ingest_ts") >= F.lit(corte_ingesta(dias)))


def hora_local(col):
    """Timestamp UTC -> hora de pared en Colombia (para id_tiempo, hora y franja)."""
    return F.from_utc_timestamp(col, ZONA_HORARIA)


SIN_REGION = "Sin región"


def region(col):
    """Región de facturación del pago; IEEE-CIS no la trae en ~11 % de los pagos (addr1 vacío)."""
    return F.coalesce(col, F.lit(SIN_REGION))


def id_tiempo(col):
    return F.date_format(hora_local(col), "yyyyMMddHH").cast("int")


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
        tx = llegadas(tx, dias)
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
    """Promedio de monto de los últimos 30 días por cliente: la referencia de R2.

    Los 30 días se cuentan hacia atrás desde el último pago registrado en Silver (`reloj_datos`)."""
    tx = spark.read.format("delta").load(tables.ruta("silver_transacciones"))
    reloj = tx.agg(F.max("ts_evento")).collect()[0][0]
    if reloj is None:
        return
    inicio = reloj - timedelta(days=PERFIL_DIAS)
    perfil = (
        tx.where((F.col("ts_evento") > F.lit(inicio)) & (F.col("ts_evento") <= F.lit(reloj)))
        .groupBy("id_cliente").agg(F.avg("monto").alias("avg_monto_30d"), F.count("*").alias("n_tx_30d"))
        .withColumn("reloj_datos", F.lit(reloj).cast("timestamp"))
        .withColumn("actualizado_ts", F.current_timestamp())
    )
    escribir(spark, perfil, "perfil_cliente")
    log.info("perfil_cliente: 30 días hasta %s (último pago en Silver)", reloj)


def dim_tiempo(spark):
    """Una fila por hora, en hora de Colombia, que cubre todos los pagos de Silver."""
    local = hora_local(F.col("ts_evento"))
    limites = spark.read.format("delta").load(tables.ruta("silver_transacciones")).agg(
        F.to_date(F.min(local)).alias("a"), F.to_date(F.max(local)).alias("b")).collect()[0]
    if limites["a"] is None:
        return
    horas = spark.sql(
        "SELECT explode(sequence(to_timestamp('%s'), to_timestamp('%s') + INTERVAL 1 DAY - INTERVAL 1 HOUR,"
        " INTERVAL 1 HOUR)) AS ts" % (limites["a"], limites["b"])
    )
    h = F.hour("ts")
    dim = horas.select(
        F.date_format("ts", "yyyyMMddHH").cast("int").alias("id_tiempo"),
        F.to_date("ts").alias("fecha"), F.year("ts").alias("anio"), F.month("ts").alias("mes"), F.dayofmonth("ts").alias("dia"),
        h.alias("hora"),
        F.when(h < 6, "madrugada").when(h < 12, "manana").when(h < 18, "tarde").otherwise("noche").alias("franja"),
        F.dayofweek("ts").alias("dia_semana"), F.weekofyear("ts").alias("semana_iso"),
        (F.dayofweek("ts").isin(1, 7)).alias("es_fin_de_semana"), F.lit(ZONA_HORARIA).alias("zona_horaria"),
    )
    escribir(spark, dim, "dim_tiempo")  # pequeña (24 filas por día): se reescribe completa


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
def _agregar_columnas(spark, df, nombre):
    """Agrega a la tabla Gold las columnas nuevas de `df` (evolución de esquema explícita, antes del MERGE)."""
    actuales = {c.name.lower() for c in spark.read.format("delta").load(ruta(nombre)).schema}
    nuevas = ["`%s` %s" % (c.name, c.dataType.simpleString()) for c in df.schema if c.name.lower() not in actuales]
    if nuevas:
        spark.sql("ALTER TABLE delta.`%s` ADD COLUMNS (%s)" % (ruta(nombre), ", ".join(nuevas)))
        log.info("%s: columnas nuevas %s", nombre, ", ".join(nuevas))


def _merge_o_crear(spark, df, nombre, condicion, particion=None, set_si_coincide=None):
    if not existe(spark, nombre):
        escribir(spark, df, nombre, particion)
        return
    _agregar_columnas(spark, df, nombre)
    m = DeltaTable.forPath(spark, ruta(nombre)).alias("t").merge(df.alias("s"), condicion)
    if set_si_coincide:
        m = m.whenMatchedUpdate(set=set_si_coincide)
    m.whenNotMatchedInsertAll().execute()


def hechos(spark, dias):
    batch_id = "gold-%s" % uuid.uuid4().hex[:8]
    silver = spark.read.format("delta").load(tables.ruta("silver_transacciones"))
    incremental = existe(spark, "fact_transacciones")  # la primera vez se toma todo Silver
    if incremental:
        silver = llegadas(silver, dias)

    # fact_transacciones: una fila por transacción de Silver; conserva partición y offset de Kafka (RNF-06)
    tx = silver.select(
        "id_transaccion", "id_cliente", "id_comercio", "id_dispositivo",
        id_tiempo("ts_evento").alias("id_tiempo"),
        "ts_evento", "fecha", "monto", "tipo", region(F.col("ciudad")).alias("region"), F.lit(False).alias("es_sospechosa"),
        # La realidad (contracargos / fraude confirmado) llega después y por otro camino: la llena `etiquetar`.
        F.lit(None).cast("boolean").alias("es_fraude_real"), F.lit(None).cast("string").alias("particion_eval"),
        "source_system", "kafka_partition", "kafka_offset", F.col("ingest_ts").alias("ingest_ts"),
        F.lit(batch_id).alias("batch_id"),
    )
    _merge_o_crear(spark, tx, "fact_transacciones", "t.id_transaccion = s.id_transaccion AND t.fecha = s.fecha", "fecha")

    # fact_alertas: una fila por (alerta, versión de reglas). Fecha e id_tiempo son los del PAGO (como en
    # fact_transacciones), no los de la alerta: al reproducir un histórico todas las alertas nacen hoy.
    ts_pago = F.coalesce("ts_evento", "ventana_inicio")   # R1 no tiene una transacción propia: inicio de su ventana
    alertas = spark.read.format("delta").load(tables.ruta("silver_alertas"))
    if incremental:
        alertas = llegadas(alertas, dias)
    al = con_llegada(alertas.join(tiempos_de_llegada(spark, dias if incremental else None), "id_transaccion", "left")).select(
        "id_alerta", "rule_version", F.col("regla").alias("id_regla"), "id_transaccion", "id_cliente", "id_comercio",
        "ts_evento", "ts_alerta", "ventana_inicio", "ventana_fin", "n_tx", "monto_total",
        F.get_json_object("detalle", "$.puntaje").cast("double").alias("puntaje"),
        *latencias(), id_tiempo(ts_pago).alias("id_tiempo"), F.to_date(ts_pago).alias("fecha"),
        F.lit(None).cast("boolean").alias("es_fraude_real"), F.lit(None).cast("string").alias("particion_eval"),
        F.lit("streaming").alias("origen"), "source_system", "kafka_partition", "kafka_offset", F.lit(batch_id).alias("batch_id"),
        F.col("ingest_ts"),
    )
    _merge_o_crear(
        spark, al, "fact_alertas", "t.id_alerta = s.id_alerta AND t.rule_version = s.rule_version", "fecha",
        set_si_coincide=dict({"n_tx": "s.n_tx", "monto_total": "s.monto_total", "ventana_fin": "s.ventana_fin"},
                             **{c: "s." + c for c in COLUMNAS_LATENCIA}),
    )
    marcar_sospechosas(spark, dias if incremental else None)
    etiquetar(spark, dias if incremental else None)
    log.info("hechos de Gold actualizados (batch %s)", batch_id)


# ---------------------------------------------------------------- latencia por tramo (D4)
# Cada pago pasa por: emisión (ts_emision, en el JSON) -> Kafka (kafka_timestamp) -> lote del streaming (ingest_ts de
# Bronze) -> alerta (ts_alerta). Todo eso ya está en Bronze, así que la latencia se mide sin tocar el streaming.
#   envío  = Kafka - emisión          (red y productor)
#   cola   = lote - Kafka             (espera en Kafka hasta que el streaming toma el mensaje; crece si hay atraso)
#   reglas = alerta - lote            (Bronze, Silver, R2/R3/R4 y escritura de la alerta)
#   total  = alerta - emisión (o - Kafka si el evento no trae ts_emision)
# Al reproducir un histórico, ts_evento es de meses atrás: por eso la latencia NO se mide contra ts_evento, salvo para
# eventos sin rastro en Bronze (R1 marca ventanas, no transacciones) y solo si la diferencia es de menos de un día.
COLUMNAS_LATENCIA = ["ts_emision", "latencia_s", "lat_envio_s", "lat_cola_s", "lat_reglas_s"]


def tiempos_de_llegada(spark, dias=None):
    """Por transacción, todas sus llegadas a Bronze: entrada al streaming, llegada a Kafka y emisión (una por envío)."""
    br = spark.read.format("delta").load(tables.ruta("bronze_transacciones")).where(F.col("id_transaccion").isNotNull())
    if dias is not None:
        br = br.where(F.col("ingest_ts") >= F.lit(corte_ingesta(dias + 1)))
    return br.groupBy("id_transaccion").agg(F.collect_list(F.struct(
        F.col("ingest_ts").alias("ts_lote"), F.col("kafka_timestamp").alias("ts_kafka"),
        F.to_timestamp(F.get_json_object("value", "$.ts_emision")).alias("ts_emision"))).alias("llegadas"))


def con_llegada(alertas):
    """Elige, para cada alerta, la llegada que la provocó: la más reciente que entró al streaming antes de la alerta.

    Con la primera llegada, un pago que se vuelve a enviar (la demo reserva y reenvía los últimos pagos) medía su
    latencia desde el primer envío, horas antes; y un duplicado que llega después de la alerta (el replayer reanudado)
    no debe contar. Deja las columnas ts_emision, ts_kafka y ts_lote que usa `latencias`."""
    elegida = F.expr("array_max(filter(llegadas, x -> x.ts_lote <= ts_alerta))")   # el struct compara por ts_lote
    return (alertas.withColumn("_llegada", elegida)
            .withColumn("ts_emision", F.col("_llegada.ts_emision")).withColumn("ts_kafka", F.col("_llegada.ts_kafka"))
            .withColumn("ts_lote", F.col("_llegada.ts_lote")).drop("_llegada", "llegadas"))


def _seg(a, b):
    return F.col(a).cast("double") - F.col(b).cast("double")


def latencias():
    origen = F.coalesce(F.col("ts_emision"), F.col("ts_kafka"))
    contra_evento = _seg("ts_alerta", "ts_evento")
    total = F.when(origen.isNotNull(), F.col("ts_alerta").cast("double") - origen.cast("double")).when(
        contra_evento < 86400, contra_evento)
    return [F.col("ts_emision"), F.round(total, 3).alias("latencia_s"),
            F.round(_seg("ts_kafka", "ts_emision"), 3).alias("lat_envio_s"),
            F.round(_seg("ts_lote", "ts_kafka"), 3).alias("lat_cola_s"),
            F.round(_seg("ts_alerta", "ts_lote"), 3).alias("lat_reglas_s")]


def _hay_archivo(uri):
    if not uri:
        return False
    if uri.startswith(("s3a://", "s3://")):
        return bool(lake.listar(uri))
    return os.path.exists(uri)


def etiquetar(spark, dias=None):
    """Cruza Gold con la realidad: `es_fraude_real` y `particion_eval` desde ETIQUETAS_URI (id_transaccion, isFraud,
    particion). Con IEEE-CIS es la etiqueta de Vesta; en un banco serían los contracargos confirmados, que llegan días
    después. El streaming nunca la ve: solo sirve para medir precisión, cobertura y falsos positivos en el tablero.
    Si el archivo no está, no hace nada. `dias`: solo lo que llegó en esos días (None = todo Gold)."""
    uri = config.env("ETIQUETAS_URI", config.lake("raw", "ieee-cis", "preparado", "etiquetas.csv.gz"))
    if not _hay_archivo(uri):
        log.info("sin etiquetas de fraude real (%s): Gold queda sin es_fraude_real", uri or "ETIQUETAS_URI vacío")
        return
    et = spark.read.option("header", True).csv(uri).select(
        "id_transaccion", (F.col("isFraud").cast("int") == 1).alias("es_fraude_real"),
        F.col("particion").alias("particion_eval"))
    tx = spark.read.format("delta").load(ruta("fact_transacciones"))
    if dias is not None:
        tx = llegadas(tx, dias)
    pendientes = tx.where(F.col("es_fraude_real").isNull()).select("id_transaccion", "fecha").join(et, "id_transaccion")
    (
        DeltaTable.forPath(spark, ruta("fact_transacciones")).alias("t")
        .merge(pendientes.alias("s"), "t.id_transaccion = s.id_transaccion AND t.fecha = s.fecha")
        .whenMatchedUpdate(set={"es_fraude_real": "s.es_fraude_real", "particion_eval": "s.particion_eval"})
        .execute()
    )
    (
        DeltaTable.forPath(spark, ruta("fact_alertas")).alias("t")
        .merge(et.alias("s"), "t.id_transaccion = s.id_transaccion")
        .whenMatchedUpdate(condition="t.es_fraude_real IS NULL",
                           set={"es_fraude_real": "s.es_fraude_real", "particion_eval": "s.particion_eval"})
        .execute()
    )
    log.info("etiquetas de fraude real cruzadas con Gold (%s)", uri)


def marcar_sospechosas(spark, dias=None):
    """Una transacción es sospechosa si tiene alerta propia (R2, R3, R4) o si cae dentro de la ventana de una R1.

    `dias`: revisa solo lo que llegó en esos días (None = todo Gold)."""
    alertas = spark.read.format("delta").load(ruta("fact_alertas")).where("origen = 'streaming'")
    tx = spark.read.format("delta").load(ruta("fact_transacciones"))
    if dias is not None:
        tx = llegadas(tx, dias)
    propias = alertas.where(F.col("id_transaccion").isNotNull()).select("id_transaccion")
    r1 = alertas.where("id_regla = 'R1'").select(
        F.col("id_cliente").alias("r_cli"), F.col("ventana_inicio").alias("v0"), F.col("ventana_fin").alias("v1"))
    en_ventana = (
        tx.select("id_transaccion", "id_cliente", "ts_evento")
        .join(r1, (F.col("id_cliente") == F.col("r_cli")) & (F.col("ts_evento") >= F.col("v0")) & (F.col("ts_evento") < F.col("v1")))
        .select("id_transaccion")
    )
    # Solo las transacciones de este rango; la partición va en la condición para que el MERGE no recorra todo Gold.
    sospechosas = (
        tx.select("id_transaccion", "fecha").join(propias.union(en_ventana).distinct(), "id_transaccion", "left_semi")
    )
    (
        DeltaTable.forPath(spark, ruta("fact_transacciones")).alias("t")
        .merge(sospechosas.alias("s"), "t.id_transaccion = s.id_transaccion AND t.fecha = s.fecha")
        .whenMatchedUpdate(condition="t.es_sospechosa = false", set={"es_sospechosa": "true"})
        .execute()
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paso", choices=["dimensiones", "hechos"])
    ap.add_argument("--dias", type=int, default=int(config.env("GOLD_DIAS_INCREMENTAL", "2")),
                    help="días de LLEGADA a Silver (ingest_ts) que se reprocesan (la primera vez se toma todo)")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = get_spark("fraudlens-gold-" + a.paso)
    try:
        fijar_marca(spark)
        {"dimensiones": dimensiones, "hechos": hechos}[a.paso](spark, a.dias)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

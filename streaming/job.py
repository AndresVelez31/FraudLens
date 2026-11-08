"""Job de Spark Structured Streaming de FraudLens (RF-04 a RF-09, RNF-03, RNF-04, RNF-08).

Dos consultas de streaming (cada una con su checkpoint en el lakehouse):

  1. `ingesta`: Kafka -> Bronze -> Silver (+ DLQ y cuarentena) y reglas R2 y R3 (join stream-static con Gold).
     Corre en foreachBatch cada 5 s; todo lo que escribe es idempotente (MERGE / PutItem condicional), así que
     si el job muere a mitad de un lote, al reiniciar desde el checkpoint no queda nada duplicado.
  2. `r1_ventanas`: lee Silver como stream (ya sin duplicados) y mantiene las ventanas deslizantes de 5 min por
     cliente con salida en modo `update`: la alerta sale en el micro-batch en que se supera el umbral, sin esperar
     al cierre de la ventana ni al watermark (RNF-04).

Uso:  python -m streaming.job [--max-segundos N]
"""
import argparse
import logging
import time

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import alerts, config, lake, pii, rules, schema, tables
from common.spark import get_spark

log = logging.getLogger("fraudlens.streaming")


# --------------------------------------------------------------------------------------------------
# Clasificación de registros (función pura: se prueba sin Kafka)
# --------------------------------------------------------------------------------------------------
def clasificar(raw):
    """`raw` tiene la columna `value` (JSON como texto). Devuelve el DataFrame con `e` (evento), `ilegible`, `motivo`.

    * ilegible = el texto no es un JSON interpretable -> topic DLQ.
    * motivo   = el JSON se leyó pero incumple una regla de calidad -> tabla de cuarentena (null si es válido).
    """
    p = raw.withColumn(
        "e", F.from_json("value", schema.EVENTO, {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": schema.CORRUPTO})
    )
    p = p.withColumn("ilegible", F.col("e").isNull() | F.col("e.%s" % schema.CORRUPTO).isNotNull())
    ts = F.to_timestamp("e.ts_evento")
    motivo = None
    # Se evalúan en orden inverso para que gane el primer motivo de la lista.
    candidatos = [("campo_obligatorio_nulo:%s" % c, F.col("e.%s" % c).isNull()) for c in schema.CAMPOS_OBLIGATORIOS]
    candidatos += [
        ("monto_no_positivo", F.col("e.monto") <= 0),
        ("ts_evento_invalido", ts.isNull()),
        ("tipo_invalido", ~F.col("e.tipo").isin(schema.TIPOS_VALIDOS)),
    ]
    for texto, condicion in reversed(candidatos):
        motivo = F.when(condicion, F.lit(texto)).otherwise(motivo if motivo is not None else F.lit(None).cast("string"))
    return p.withColumn("motivo", F.when(F.col("ilegible"), F.lit(None).cast("string")).otherwise(motivo))


def a_silver(validas, batch_id):
    """Estandariza (tipos, fecha) y reemplaza datos personales por HMAC."""
    e = "e."
    return validas.select(
        F.col(e + "id_transaccion").alias("id_transaccion"),
        F.to_timestamp(e + "ts_evento").alias("ts_evento"),
        F.to_date(F.to_timestamp(e + "ts_evento")).alias("fecha"),
        F.col(e + "id_cliente").alias("id_cliente"),
        F.col(e + "id_comercio").alias("id_comercio"),
        F.col(e + "id_dispositivo").alias("id_dispositivo"),
        F.col(e + "monto").alias("monto"),
        F.upper(F.col(e + "tipo")).alias("tipo"),
        F.col(e + "ciudad").alias("ciudad"),
        pii.hmac_col(F.col(e + "documento")).alias("documento_h"),
        pii.hmac_col(F.col(e + "telefono")).alias("telefono_h"),
        pii.hmac_col(F.col(e + "correo")).alias("correo_h"),
        F.lit(config.SOURCE_SYSTEM).alias("source_system"),
        "kafka_topic", "kafka_partition", "kafka_offset", "ingest_ts",
        F.lit(str(batch_id)).alias("batch_id"),
    ).dropDuplicates(["id_transaccion"])


def _existe(spark, path):
    return DeltaTable.isDeltaTable(spark, path)


# --------------------------------------------------------------------------------------------------
# Consulta 1: ingesta + R2 + R3
# --------------------------------------------------------------------------------------------------
def procesar_lote(spark):
    version = config.RULE_VERSION

    def _lote(batch_df, batch_id):
        t0 = time.time()
        raw = (
            batch_df.select(
                F.col("key").cast("string").alias("key"),
                F.col("value").cast("string").alias("value"),
                F.col("topic").alias("kafka_topic"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("timestamp").alias("kafka_timestamp"),
            )
            .withColumn("id_transaccion", F.get_json_object("value", "$.id_transaccion"))
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("batch_id", F.lit(str(batch_id)))
            .withColumn("source_system", F.lit("kafka"))
            .withColumn("fecha_ingesta", F.to_date("ingest_ts"))
            .cache()
        )
        n_raw = raw.count()
        if n_raw == 0:
            raw.unpersist()
            return
        tiempos = {}

        def marca(nombre, desde=[time.time()]):  # noqa: B006  (cronómetro por paso, para el log)
            ahora = time.time()
            tiempos[nombre] = ahora - desde[0]
            desde[0] = ahora

        # 1) Bronze: el evento crudo, sin tocar, con su topic/partición/offset (RF-04). Admite duplicados.
        raw.write.format("delta").mode("append").save(tables.ruta("bronze_transacciones"))
        marca("bronze")

        c = clasificar(raw).cache()
        # Un solo recorrido para los tres conteos (cada .count() suelto es un job de Spark entero).
        cuentas = c.agg(
            F.sum(F.col("ilegible").cast("int")).alias("ilegibles"),
            F.sum((~F.col("ilegible") & F.col("motivo").isNotNull()).cast("int")).alias("cuarentena"),
        ).collect()[0]
        n_dlq, n_cuar = int(cuentas["ilegibles"] or 0), int(cuentas["cuarentena"] or 0)
        marca("clasificar")

        # 2) JSON ilegible -> DLQ (RF-05). Se publica el texto original con la misma clave.
        ilegibles = c.where("ilegible").select("key", "value")
        if n_dlq:
            (
                ilegibles.write.format("kafka")
                .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
                .option("topic", config.KAFKA_DLQ_TOPIC)
                .save()
            )

        # 3) Incumple calidad -> cuarentena con el motivo (RF-05, RNF-08).
        cuarentena = c.where(~F.col("ilegible") & F.col("motivo").isNotNull())
        if n_cuar:
            cuarentena.select(
                "value", "motivo", "id_transaccion", "kafka_topic", "kafka_partition", "kafka_offset",
                "ingest_ts", "batch_id", "source_system",
            ).write.format("delta").mode("append").save(tables.ruta("silver_cuarentena"))

        # 4) Válidos -> Silver con MERGE por id_transaccion (deduplica contra todo lo anterior).
        marca("dlq_cuarentena")
        validas = a_silver(c.where(~F.col("ilegible") & F.col("motivo").isNull()), batch_id).cache()
        marca("silver_prep")
        if n_raw - n_dlq - n_cuar > 0:
            (
                DeltaTable.forPath(spark, tables.ruta("silver_transacciones"))
                .alias("t")
                .merge(validas.alias("s"), "t.id_transaccion = s.id_transaccion AND t.fecha = s.fecha")
                .whenNotMatchedInsertAll()
                .execute()
            )
            n_validas = validas.count()  # ya está en caché tras el MERGE
            marca("silver_merge")

            # 5) R2 y R3: join stream-static contra el Gold / lista de riesgo vigentes en este instante.
            tx = validas.select(
                "id_transaccion", "ts_evento", "id_cliente", "id_comercio", "id_dispositivo", "monto", "tipo",
                "kafka_partition", "kafka_offset",
            )
            partes = []
            ruta_disp, ruta_perfil = config.lake("gold", "dim_dispositivo"), config.lake("gold", "perfil_cliente")
            if _existe(spark, ruta_disp) and _existe(spark, ruta_perfil):
                partes.append(
                    rules.r2_alertas(tx, spark.read.format("delta").load(ruta_disp), spark.read.format("delta").load(ruta_perfil), version)
                )
            else:
                log.warning("R2 omitida: Gold aún no tiene dim_dispositivo/perfil_cliente (corre fraude_pipeline)")
            ruta_lista = config.lake("silver", "lista_riesgo")
            if _existe(spark, ruta_lista):
                lista = rules.lista_vigente(spark.read.format("delta").load(ruta_lista))
                partes.append(rules.r3_alertas(tx, lista, version))
            else:
                log.warning("R3 omitida: no hay lista de riesgo cargada (corre catalogos_diario)")
            if partes:
                alertas = partes[0]
                for p in partes[1:]:
                    alertas = alertas.unionByName(p)
                total, nuevas = alerts.publicar(spark, alertas, batch_id)
                if total:
                    log.info("lote %s: %d alertas R2/R3 (%d nuevas)", batch_id, total, nuevas)

        marca("reglas")
        n_validas = locals().get("n_validas", 0)
        log.info(
            "lote %s: raw=%d validas=%d cuarentena=%d dlq=%d (%.1fs) %s", batch_id, n_raw, n_validas, n_cuar, n_dlq,
            time.time() - t0, " ".join("%s=%.1f" % kv for kv in tiempos.items()),
        )
        validas.unpersist()
        c.unpersist()
        raw.unpersist()

    return _lote


# --------------------------------------------------------------------------------------------------
# Consulta 2: R1 con ventanas deslizantes y estado
# --------------------------------------------------------------------------------------------------
def procesar_r1(spark):
    def _lote(agregado, batch_id):
        total, nuevas = alerts.publicar(spark, rules.r1_alertas(agregado, config.RULE_VERSION), "r1-%s" % batch_id)
        if total:
            log.info("R1 lote %s: %d alertas (%d nuevas)", batch_id, total, nuevas)

    return _lote


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-segundos", type=int, default=0, help="Termina tras N segundos (pruebas). 0 = indefinido")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    spark = get_spark("fraudlens-streaming")
    spark.conf.set("spark.sql.streaming.stopGracefullyOnShutdown", "true")
    config.pii_key()  # falla pronto si falta la clave
    tables.asegurar_tablas(spark)

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", config.env("STARTING_OFFSETS", "earliest"))
        .option("maxOffsetsPerTrigger", config.env("MAX_OFFSETS_PER_TRIGGER", "50000"))
        .option("failOnDataLoss", "false")
        .load()
    )
    trigger = config.env("TRIGGER_SEGUNDOS", "5") + " seconds"

    q_ingesta = (
        kafka.writeStream.queryName("ingesta")
        .foreachBatch(procesar_lote(spark))
        .option("checkpointLocation", config.lake("checkpoints", "ingesta"))
        .trigger(processingTime=trigger)
        .start()
    )

    silver = spark.readStream.format("delta").load(tables.ruta("silver_transacciones"))
    r1 = rules.r1_agregar(silver.select("ts_evento", "id_cliente", "monto").withWatermark("ts_evento", "10 minutes"))
    q_r1 = (
        r1.writeStream.queryName("r1_ventanas")
        .outputMode("update")
        .foreachBatch(procesar_r1(spark))
        .option("checkpointLocation", config.lake("checkpoints", "r1"))
        .trigger(processingTime=config.env("TRIGGER_R1_SEGUNDOS", "2") + " seconds")  # R1 lee de Silver: sondea más seguido
        .start()
    )

    # Latido: el sensor `check_streaming_vivo` de Airflow lo lee para saber que el job sigue vivo.
    inicio = time.time()
    while True:
        for q in (q_ingesta, q_r1):
            if q.exception():
                raise q.exception()  # el contenedor se reinicia y retoma desde el checkpoint
        lake.escribir_latido({"queries": [q_ingesta.name, q_r1.name], "activas": all(q.isActive for q in (q_ingesta, q_r1))})
        if args.max_segundos and time.time() - inicio > args.max_segundos:
            break
        time.sleep(10)
    q_ingesta.stop()
    q_r1.stop()
    spark.stop()


if __name__ == "__main__":
    main()

"""Creación idempotente de las tablas Delta de Bronze y Silver con su historial de 90 días (RNF-06)."""
from common import config

RETENCION = (
    "TBLPROPERTIES ('delta.logRetentionDuration'='interval 90 days',"
    " 'delta.deletedFileRetentionDuration'='interval 90 days')"
)

KAFKA_COLS = "kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT"

TABLAS = {
    # nombre: (ruta, columnas, particion)
    "bronze_transacciones": (
        config.lake("bronze", "transacciones"),
        "key STRING, value STRING, id_transaccion STRING, %s, kafka_timestamp TIMESTAMP, ingest_ts TIMESTAMP,"
        " batch_id STRING, source_system STRING, fecha_ingesta DATE" % KAFKA_COLS,
        "fecha_ingesta",
    ),
    "silver_transacciones": (
        config.lake("silver", "transacciones"),
        "id_transaccion STRING, ts_evento TIMESTAMP, fecha DATE, id_cliente STRING, id_comercio STRING,"
        " id_dispositivo STRING, monto DOUBLE, tipo STRING, ciudad STRING, documento_h STRING, telefono_h STRING,"
        " correo_h STRING, source_system STRING, %s, ingest_ts TIMESTAMP, batch_id STRING" % KAFKA_COLS,
        "fecha",
    ),
    "silver_cuarentena": (
        config.lake("silver", "cuarentena"),
        "value STRING, motivo STRING, id_transaccion STRING, %s, ingest_ts TIMESTAMP, batch_id STRING,"
        " source_system STRING" % KAFKA_COLS,
        None,
    ),
    "silver_alertas": (
        config.lake("silver", "alertas"),
        "id_alerta STRING, regla STRING, rule_version STRING, id_transaccion STRING, id_cliente STRING,"
        " id_comercio STRING, ts_evento TIMESTAMP, ts_alerta TIMESTAMP, ventana_inicio TIMESTAMP,"
        " ventana_fin TIMESTAMP, n_tx BIGINT, monto_total DOUBLE, detalle STRING, kafka_partition INT,"
        " kafka_offset BIGINT, batch_id STRING, source_system STRING, ingest_ts TIMESTAMP",
        "regla",   # cada consulta del streaming escribe sus reglas en su propia partición (D1, hallazgo 3)
    ),
}


def ruta(nombre):
    return TABLAS[nombre][0]


def asegurar_tablas(spark, nombres=None):
    for nombre in nombres or TABLAS:
        path, cols, part = TABLAS[nombre]
        particion = "PARTITIONED BY (%s)" % part if part else ""
        spark.sql("CREATE TABLE IF NOT EXISTS delta.`%s` (%s) USING DELTA %s %s" % (path, cols, particion, RETENCION))


def id_de_escritor(nombre):
    """Identificador estable de un escritor del streaming, guardado junto a su checkpoint (D1, hallazgo 5).

    Las escrituras de Bronze y cuarentena usan txnAppId=<este id> y txnVersion=<batch_id>: si Spark repite un lote
    tras una caída, Delta reconoce la pareja y no vuelve a escribir. El id vive DENTRO de la carpeta del checkpoint:
    si se borra el checkpoint (los batch_id vuelven a 0), el id cambia y las escrituras nuevas no se confunden con
    las viejas (con el mismo id, Delta las descartaría en silencio)."""
    import uuid

    from common import lake

    uri = config.lake("checkpoints", nombre, "_fraudlens_txn_app_id")
    actual = (lake.leer_texto(uri) or "").strip()
    if actual:
        return actual
    nuevo = "fraudlens-%s-%s" % (nombre, uuid.uuid4().hex[:12])
    lake.escribir_texto(uri, nuevo)
    return nuevo


def escribir_una_vez(df, ruta_tabla, app_id, batch_id):
    """Append idempotente por micro-batch (opciones transaccionales de Delta)."""
    (df.write.format("delta").mode("append").option("txnAppId", app_id).option("txnVersion", int(batch_id))
     .save(ruta_tabla))


def fijar_retencion(spark, path):
    """Para tablas creadas por escritura directa (Gold): historial de 90 días."""
    spark.sql(
        "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 90 days',"
        " 'delta.deletedFileRetentionDuration'='interval 90 days')" % path
    )

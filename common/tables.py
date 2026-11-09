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
        None,
    ),
}


def ruta(nombre):
    return TABLAS[nombre][0]


def asegurar_tablas(spark, nombres=None):
    for nombre in nombres or TABLAS:
        path, cols, part = TABLAS[nombre]
        particion = "PARTITIONED BY (%s)" % part if part else ""
        spark.sql("CREATE TABLE IF NOT EXISTS delta.`%s` (%s) USING DELTA %s %s" % (path, cols, particion, RETENCION))


def fijar_retencion(spark, path):
    """Para tablas creadas por escritura directa (Gold): historial de 90 días."""
    spark.sql(
        "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 90 days',"
        " 'delta.deletedFileRetentionDuration'='interval 90 days')" % path
    )

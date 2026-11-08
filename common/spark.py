"""Sesión de Spark con Delta Lake y acceso a S3 (o MinIO)."""
import glob
import os

from common import config


def get_spark(nombre, shuffle_partitions=None):
    # Todo el sistema trabaja en UTC (eventos, ventanas y alertas).
    os.environ.setdefault("TZ", "UTC")
    from pyspark.sql import SparkSession

    b = (
        SparkSession.builder.appName(nombre)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions or config.env("SPARK_SHUFFLE_PARTITIONS", "4")))
        .config("spark.ui.enabled", config.env("SPARK_UI_ENABLED", "false"))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.databricks.delta.snapshotPartitions", config.env("DELTA_SNAPSHOT_PARTITIONS", "4"))
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")  # los cambios de esquema son explícitos
    )
    master = config.env("SPARK_MASTER")
    if master:
        b = b.master(master)

    # Jars sueltos (solo para correr sin Docker; en las imágenes ya están en $SPARK_HOME/jars).
    extra = config.env("SPARK_EXTRA_JARS")
    if extra:
        jars = [j for patron in extra.split(",") for j in glob.glob(patron)]
        b = b.config("spark.jars", ",".join(jars))

    if config.S3_ENDPOINT:  # MinIO
        b = (
            b.config("spark.hadoop.fs.s3a.endpoint", config.S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(config.S3_ENDPOINT.startswith("https")).lower())
            .config("spark.hadoop.fs.s3a.access.key", config.env("AWS_ACCESS_KEY_ID", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", config.env("AWS_SECRET_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        )
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel(config.env("SPARK_LOG_LEVEL", "WARN"))
    return spark

"""Regla R4: puntaje del modelo LightGBM "contrato" en el streaming (tarea D5).

En cada micro-batch:
  1. Se lee de Silver la historia de los clientes y dispositivos del lote (lo que ya pasó antes).
  2. Se calculan las 17 variables con common/variables.py, el MISMO código con el que se entrenó el modelo.
  3. Se puntúa cada transacción del lote; si el puntaje supera el umbral, sale una alerta R4.

El modelo y su umbral viven en el lakehouse, en models/r4/ (lightgbm_contrato.txt y lightgbm_contrato.json). Se
publican con `python -m common.r4 publicar` después de entrenar con ml/modelo_lightgbm.py. Si no están, R4 se omite
y el streaming sigue con R1 a R3.

Costo: la historia se lee completa por cliente y dispositivo, sin límite de fechas, porque el modelo usa el número
total de transacciones previas. Con el volumen de la demo (cientos de miles de filas en Silver) es aceptable; en
producción la historia por cliente iría en una tabla de estado (por ejemplo, DynamoDB) actualizada en cada lote.
"""
import io
import json
import logging
import os
import sys
from urllib.parse import urlparse

from common import config

log = logging.getLogger("fraudlens.r4")

ARCHIVO_MODELO = "lightgbm_contrato.txt"
ARCHIVO_UMBRAL = "lightgbm_contrato.json"
MODELO_LOCAL = "data/modelos"   # donde lo deja ml/modelo_lightgbm.py


# ------------------------------------------------------------------ modelo en el lakehouse
def _s3():
    import boto3

    return boto3.client("s3", region_name=config.REGION, endpoint_url=config.S3_ENDPOINT)


def _ruta(nombre):
    return config.lake("models", "r4", nombre)


def _leer(ruta):
    """Bytes de un archivo del lakehouse (s3a://bucket/llave o ruta local). None si no existe."""
    u = urlparse(ruta)
    if u.scheme in ("s3a", "s3"):
        try:
            return _s3().get_object(Bucket=u.netloc, Key=u.path.lstrip("/"))["Body"].read()
        except Exception as e:  # noqa: BLE001  (NoSuchKey, sin permisos...: R4 se omite y se registra)
            log.warning("No se pudo leer %s: %s", ruta, e)
            return None
    ruta = u.path if u.scheme == "file" else ruta
    if not os.path.exists(ruta):
        return None
    with open(ruta, "rb") as f:
        return f.read()


class Modelo:
    def __init__(self, texto_modelo, umbral):
        import lightgbm as lgb

        self.booster = lgb.Booster(model_str=texto_modelo)
        self.umbral = float(umbral["umbral"])
        self.columnas = umbral["variables"]

    def puntuar(self, x):
        return self.booster.predict(x[self.columnas])


def cargar():
    """El modelo publicado en models/r4/, o None si todavía no se publicó."""
    texto, umbral = _leer(_ruta(ARCHIVO_MODELO)), _leer(_ruta(ARCHIVO_UMBRAL))
    if texto is None or umbral is None:
        return None
    m = Modelo(texto.decode("utf-8"), json.loads(umbral))
    log.info("R4 activa: modelo %s, umbral %.4f", _ruta(ARCHIVO_MODELO), m.umbral)
    return m


def publicar(origen=MODELO_LOCAL):
    """Sube el modelo entrenado y su umbral al lakehouse (models/r4/)."""
    for nombre in (ARCHIVO_MODELO, ARCHIVO_UMBRAL):
        ruta, local = _ruta(nombre), os.path.join(origen, nombre)
        u = urlparse(ruta)
        with open(local, "rb") as f:
            datos = f.read()
        if u.scheme in ("s3a", "s3"):
            _s3().put_object(Bucket=u.netloc, Key=u.path.lstrip("/"), Body=io.BytesIO(datos))
        else:
            os.makedirs(os.path.dirname(ruta), exist_ok=True)
            with open(ruta, "wb") as f:
                f.write(datos)
        print("publicado", ruta, len(datos), "bytes")


# ------------------------------------------------------------------ puntaje de un micro-batch
def alertas(spark, tx, silver, modelo, version):
    """Alertas R4 del lote `tx` (DataFrame de Spark con id_transaccion, ts_evento, id_cliente, id_comercio,
    id_dispositivo, monto, tipo, region, correo_dominio, kafka_partition, kafka_offset).

    `silver`: la tabla silver_transacciones (ya incluye el lote, porque el MERGE corre antes)."""
    import pandas as pd
    from pyspark.sql import functions as F

    from common import rules, variables

    lote = tx.toPandas()
    if lote.empty:
        return None
    clientes = spark.createDataFrame([(c,) for c in lote["id_cliente"].dropna().unique()], "id_cliente string")
    disp = lote["id_dispositivo"].dropna().unique()
    previas = silver.join(F.broadcast(clientes), "id_cliente", "left_semi")
    if len(disp):
        dispositivos = spark.createDataFrame([(d,) for d in disp], "id_dispositivo string")
        previas = previas.unionByName(silver.join(F.broadcast(dispositivos), "id_dispositivo", "left_semi"))
    historia = (previas.select("id_transaccion", "ts_evento", "id_cliente", "id_dispositivo", "monto", "tipo",
                               F.col("ciudad").alias("region"))
                .withColumn("correo_dominio", F.lit(None).cast("string"))   # solo se usa el del lote
                .dropDuplicates(["id_transaccion"]).toPandas())
    historia = historia[~historia["id_transaccion"].isin(lote["id_transaccion"])]
    cols = ["id_transaccion", "ts_evento", "id_cliente", "id_dispositivo", "monto", "tipo", "region", "correo_dominio"]
    todo = pd.concat([historia[cols], lote[cols]], ignore_index=True)
    todo["t"] = variables.segundos_desde_base(todo["ts_evento"])
    x = variables.calcular(todo)
    es_lote = todo["id_transaccion"].isin(lote["id_transaccion"]).to_numpy() & (todo.index >= len(historia))
    puntaje = pd.Series(modelo.puntuar(x[es_lote]), index=todo.loc[es_lote, "id_transaccion"].to_numpy())
    lote["puntaje"] = lote["id_transaccion"].map(puntaje)
    altos = lote[lote["puntaje"] >= modelo.umbral]
    log.info("R4: %d transacciones puntuadas con %d filas de historia; %d sobre el umbral %.4f",
             len(lote), len(historia), len(altos), modelo.umbral)
    if altos.empty:
        return None
    filas = altos[["id_transaccion", "ts_evento", "id_cliente", "id_comercio", "monto", "puntaje",
                   "kafka_partition", "kafka_offset"]]
    df = spark.createDataFrame(
        filas, "id_transaccion string, ts_evento timestamp, id_cliente string, id_comercio string, monto double,"
               " puntaje double, kafka_partition int, kafka_offset bigint")
    return rules.r4_alertas(df, modelo.umbral, version)


if __name__ == "__main__":
    if sys.argv[1:2] != ["publicar"]:
        sys.exit("uso: python -m common.r4 publicar [carpeta con el modelo]")
    publicar(*(sys.argv[2:3] or []))

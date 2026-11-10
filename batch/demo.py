"""Prepara la demo en vivo con el histórico ya cargado.

Los 590.540 pagos de IEEE-CIS ya pasaron por el sistema. Si se reenvían, el MERGE de Silver los descarta y DynamoDB no
acepta sus alertas otra vez, que es justo lo que muestra la idempotencia, pero no se ve nada nuevo. Para que en la demo
lleguen pagos de verdad, `reservar` saca de Silver, de las alertas de Silver y de DynamoDB los ÚLTIMOS N pagos del
dataset (los más recientes; ningún pago posterior depende de ellos). En la demo, `python -m producer.replayer
--ultimos N` los envía y el sistema los procesa como la primera vez: misma historia del cliente, misma etiqueta, misma
tasa de alertas. Gold no se toca: al volver a llegar, el MERGE actualiza sus filas.

Es reversible: Silver es Delta con 90 días de historial (RESTORE a la versión anterior) y las alertas se vuelven a
crear al llegar los pagos.

Las alertas de esos pagos en DynamoDB las borra el portátil (scripts/demo_aws.sh), con la sesión del equipo: el rol de
las EC2 tiene mínimo privilegio y no puede recorrer la tabla. Por eso este módulo deja la lista de ids en el lake
(`demo/ids_demo.txt`).

  python -m batch.demo reservar --n 2000
  python -m batch.demo limpiar --sufijo ensayo1     # quita pagos de prueba enviados con un sufijo en el id
"""
import argparse
import collections
import csv
import logging
import sys

from common import config, tables

log = logging.getLogger("fraudlens.demo")
ORIGEN_DEFECTO = "raw/ieee-cis/preparado/eventos.csv.gz"


def ultimos_ids(filas, n, particion="prueba"):
    """Ids de las últimas `n` filas de la partición, en el orden del archivo (el mismo que usa el replayer)."""
    cola = collections.deque(maxlen=n)
    for fila in filas:
        if particion == "todo" or fila["particion"] == particion:
            cola.append(fila["id_transaccion"])
    return list(cola)


def borrar_en_delta(spark, ruta, condicion_df, columna="id_transaccion"):
    """Borra de la tabla Delta las filas cuyo id está en `condicion_df` (una columna `columna`). Devuelve cuántas."""
    from delta.tables import DeltaTable

    tabla = DeltaTable.forPath(spark, ruta)
    antes = tabla.toDF().count()
    tabla.alias("t").merge(condicion_df.alias("s"), "t.%s = s.%s" % (columna, columna)).whenMatchedDelete().execute()
    return antes - DeltaTable.forPath(spark, ruta).toDF().count()


def guardar_ids(ids, uri):
    """Deja la lista de ids (uno por línea) en el lake o en un archivo local."""
    texto = "\n".join(ids) + "\n"
    if uri.startswith(("s3a://", "s3://")):
        import boto3

        bucket, clave = uri.split("://", 1)[1].split("/", 1)
        boto3.client("s3", region_name=config.REGION, endpoint_url=config.S3_ENDPOINT).put_object(
            Bucket=bucket, Key=clave, Body=texto.encode("utf-8"))
    else:
        with open(uri, "w", encoding="utf-8") as f:
            f.write(texto)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paso", choices=["reservar", "limpiar"])
    ap.add_argument("--n", type=int, default=2000, help="reservar: cuántos pagos del final de la partición")
    ap.add_argument("--particion", default="prueba", choices=["entrenamiento", "prueba", "todo"])
    ap.add_argument("--origen", default=config.lake(ORIGEN_DEFECTO))
    ap.add_argument("--sufijo", help="limpiar: sufijo de los ids de prueba (sin el guion)")
    ap.add_argument("--ids", default=config.lake("demo", "ids_demo.txt"), help="dónde se deja la lista de ids")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    from common.spark import get_spark
    spark = get_spark("fraudlens-demo-" + a.paso)
    try:
        if a.paso == "reservar":
            from producer.replayer import abrir

            ids = ultimos_ids(csv.DictReader(abrir(a.origen)), a.n, a.particion)
        else:
            if not a.sufijo:
                ap.error("limpiar necesita --sufijo")
            fin = "-" + a.sufijo
            from batch.gold import existe, ruta as ruta_gold
            rutas = [tables.ruta(n) for n in ["silver_transacciones", "silver_alertas"]]
            rutas += [ruta_gold(n) for n in ["fact_transacciones", "fact_alertas"] if existe(spark, n)]
            ids = set()
            for ruta in rutas:
                t = spark.read.format("delta").load(ruta)
                ids |= {r[0] for r in t.where(t.id_transaccion.endswith(fin)).select("id_transaccion").distinct().collect()}
            ids = sorted(ids)
        # La lista va en memoria (unos miles de ids): un DataFrame leído de Silver se vaciaría al borrar de Silver.
        ids_df = spark.createDataFrame([(i,) for i in ids], "id_transaccion STRING")
        log.info("pagos de la demo: %d", len(ids))
        for nombre in ["silver_transacciones", "silver_alertas"]:
            log.info("%s: %d filas borradas", nombre, borrar_en_delta(spark, tables.ruta(nombre), ids_df))
        if a.paso == "limpiar":   # los pagos de prueba no deben quedar en Gold (no son del dataset)
            from batch.gold import existe, ruta as ruta_gold
            for nombre in ["fact_transacciones", "fact_alertas"]:
                if existe(spark, nombre):
                    log.info("%s: %d filas borradas", nombre, borrar_en_delta(spark, ruta_gold(nombre), ids_df))
        guardar_ids(ids, a.ids)
        log.info("ids guardados en %s (las alertas de DynamoDB las borra demo_aws.sh)", a.ids)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Prepara la demo en vivo con el histórico ya cargado.

Los 590.540 pagos de IEEE-CIS ya pasaron por el sistema. Si se reenvían, el MERGE de Silver los descarta y DynamoDB no
acepta sus alertas otra vez, que es justo lo que muestra la idempotencia, pero no se ve nada nuevo. Para que en la demo
lleguen pagos de verdad, `reservar` saca de Silver, de las alertas de Silver y de DynamoDB los ÚLTIMOS N pagos del
dataset (los más recientes; ningún pago posterior depende de ellos). En la demo, `python -m producer.replayer
--ultimos N` los envía y el sistema los procesa como la primera vez: misma historia del cliente, misma etiqueta, misma
tasa de alertas. Gold no se toca: al volver a llegar, el MERGE actualiza sus filas.

Es reversible: Silver es Delta con 90 días de historial (RESTORE a la versión anterior) y las alertas se vuelven a
crear al llegar los pagos.

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


def borrar_en_dynamodb(es_de_la_demo):
    """Borra de DynamoDB las alertas cuyo id_transaccion cumple `es_de_la_demo`. Devuelve cuántas."""
    from common.alerts import _tabla_dynamodb

    tabla = _tabla_dynamodb()
    kw = {"ProjectionExpression": "id_alerta, id_transaccion"}
    borrar = []
    while True:
        r = tabla.scan(**kw)
        borrar += [i["id_alerta"] for i in r["Items"] if es_de_la_demo(i.get("id_transaccion") or "")]
        if "LastEvaluatedKey" not in r:
            break
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    with tabla.batch_writer() as lote:
        for id_alerta in borrar:
            lote.delete_item(Key={"id_alerta": id_alerta})
    return len(borrar)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paso", choices=["reservar", "limpiar"])
    ap.add_argument("--n", type=int, default=2000, help="reservar: cuántos pagos del final de la partición")
    ap.add_argument("--particion", default="prueba", choices=["entrenamiento", "prueba", "todo"])
    ap.add_argument("--origen", default=config.lake(ORIGEN_DEFECTO))
    ap.add_argument("--sufijo", help="limpiar: sufijo de los ids de prueba (sin el guion)")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    from common.spark import get_spark
    spark = get_spark("fraudlens-demo-" + a.paso)
    try:
        if a.paso == "reservar":
            from producer.replayer import abrir

            ids = ultimos_ids(csv.DictReader(abrir(a.origen)), a.n, a.particion)
            ids_df = spark.createDataFrame([(i,) for i in ids], "id_transaccion STRING")
            conjunto = set(ids)
            es_de_la_demo = conjunto.__contains__
            destinos = ["silver_transacciones", "silver_alertas"]
        else:
            if not a.sufijo:
                ap.error("limpiar necesita --sufijo")
            fin = "-" + a.sufijo
            es_de_la_demo = lambda i: i.endswith(fin)  # noqa: E731
            silver = spark.read.format("delta").load(tables.ruta("silver_transacciones"))
            ids_df = silver.where(silver.id_transaccion.endswith(fin)).select("id_transaccion").distinct()
            destinos = ["silver_transacciones", "silver_alertas"]

        ids_df = ids_df.cache()
        log.info("pagos de la demo: %d", ids_df.count())
        for nombre in destinos:
            log.info("%s: %d filas borradas", nombre, borrar_en_delta(spark, tables.ruta(nombre), ids_df))
        if a.paso == "limpiar":   # los pagos de prueba no deben quedar en Gold (no son del dataset)
            from batch.gold import existe, ruta as ruta_gold
            for nombre in ["fact_transacciones", "fact_alertas"]:
                if existe(spark, nombre):
                    log.info("%s: %d filas borradas", nombre, borrar_en_delta(spark, ruta_gold(nombre), ids_df))
        log.info("DynamoDB: %d alertas borradas", borrar_en_dynamodb(es_de_la_demo))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Ejecuta las consultas P4-P7 sobre Gold con Spark SQL (RF-11 a RF-13).

Usa los mismos archivos .sql que Amazon Athena (dashboards/athena/): se registran las tablas de Gold como vistas con
los nombres del catálogo de Glue (fact_transacciones, dim_comercio, ...). Es el plan B si Athena no está disponible.

  python -m batch.consultas p4|p5|p6|p7|p7r|todas
"""
import argparse
import os
import sys

from common import config
from common.spark import get_spark

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONSULTAS = {
    "p4": "p4_tasa_fraude.sql",
    "p5": "p5_latencia_alertas.sql",
    "p6": "p6_top10_comercios.sql",
    "p7": "p7_acierto.sql",
    "p7r": "p7_acierto_por_regla.sql",
}
TABLAS_GOLD = ["fact_transacciones", "fact_alertas", "dim_cliente", "dim_comercio", "dim_dispositivo", "dim_tiempo", "dim_regla"]


def registrar_vistas(spark):
    for t in TABLAS_GOLD:
        spark.read.format("delta").load(config.lake("gold", t)).createOrReplaceTempView(t)


def sql_de(nombre):
    with open(os.path.join(RAIZ, "dashboards", "athena", CONSULTAS[nombre]), encoding="utf-8") as f:
        return f.read().replace("approx_percentile(", "percentile_approx(")


def ejecutar(spark, nombre):
    return spark.sql(sql_de(nombre))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("consulta", choices=list(CONSULTAS) + ["todas"])
    ap.add_argument("--filas", type=int, default=20)
    a = ap.parse_args()
    spark = get_spark("fraudlens-consultas")
    registrar_vistas(spark)
    for n in (list(CONSULTAS) if a.consulta == "todas" else [a.consulta]):
        print("\n=== %s ===" % n.upper())
        ejecutar(spark, n).show(a.filas, truncate=False)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

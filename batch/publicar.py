"""Pasos finales de `fraude_pipeline`: compactar Gold, registrarlo en Athena/Glue y refrescar Metabase (RF-14, RNF-04).

  python -m batch.publicar optimize    OPTIMIZE + ZORDER de los hechos (consultas de 30 días < 10 s en Athena)
  python -m batch.publicar glue        crea en Glue las tablas Delta de Gold vía Athena (solo en AWS)
  python -m batch.publicar metabase    pide a Metabase que vuelva a leer el esquema (si METABASE_URL está definida)
"""
import argparse
import json
import logging
import sys
import time
import urllib.request

from delta.tables import DeltaTable

from common import config
from common.spark import get_spark

log = logging.getLogger("fraudlens.publicar")
TABLAS = ["dim_cliente", "dim_comercio", "dim_dispositivo", "dim_tiempo", "dim_regla", "perfil_cliente",
          "fact_transacciones", "fact_alertas"]
ZORDER = {"fact_transacciones": "id_cliente", "fact_alertas": "id_cliente"}


def optimize(spark):
    for t, col in ZORDER.items():
        ruta = config.lake("gold", t)
        if DeltaTable.isDeltaTable(spark, ruta):
            DeltaTable.forPath(spark, ruta).optimize().executeZOrderBy(col)
            log.info("OPTIMIZE ZORDER BY %s: %s", col, t)


def registrar_glue():
    """En local no hay Athena: se omite sin error."""
    if config.S3_ENDPOINT or not config.env("ATHENA_WORKGROUP"):
        log.info("registro en Glue omitido (modo local)")
        return
    import boto3

    athena = boto3.client("athena", region_name=config.REGION)
    bd, wg = config.env("GLUE_DATABASE", "fraudlens"), config.env("ATHENA_WORKGROUP")
    for t in TABLAS:
        ddl = "CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s LOCATION '%s/' TBLPROPERTIES ('table_type'='DELTA')" % (
            bd, t, config.lake("gold", t).replace("s3a://", "s3://"))
        q = athena.start_query_execution(QueryString=ddl, WorkGroup=wg)["QueryExecutionId"]
        while True:
            estado = athena.get_query_execution(QueryExecutionId=q)["QueryExecution"]["Status"]
            if estado["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)
        if estado["State"] != "SUCCEEDED":
            raise RuntimeError("Athena no pudo registrar %s: %s" % (t, estado.get("StateChangeReason")))
        log.info("Glue/Athena: tabla %s registrada", t)


def _post(url, cuerpo=None, token=None):
    req = urllib.request.Request(url, data=json.dumps(cuerpo or {}).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("X-Metabase-Session", token)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read() or b"{}")


def refrescar_metabase():
    url = config.env("METABASE_URL")
    if not url:
        log.info("METABASE_URL no definida: se omite el refresco")
        return
    token = _post(url + "/api/session", {"username": config.env("METABASE_USER"), "password": config.env("METABASE_PASSWORD")})["id"]
    bd = config.env("METABASE_DB_ID", "2")
    _post("%s/api/database/%s/sync_schema" % (url, bd), token=token)
    log.info("Metabase: sincronización de esquema solicitada (base %s)", bd)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paso", choices=["optimize", "glue", "metabase"])
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if a.paso == "optimize":
        spark = get_spark("fraudlens-optimize")
        try:
            optimize(spark)
        finally:
            spark.stop()
    elif a.paso == "glue":
        registrar_glue()
    else:
        refrescar_metabase()
    return 0


if __name__ == "__main__":
    sys.exit(main())

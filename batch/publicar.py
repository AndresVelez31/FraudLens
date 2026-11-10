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


def estado_en_glue(tabla_glue):
    """Clasifica la entrada de Glue de una tabla Delta: "falta", "rota" o "ok".

    Cuando Athena falla al registrar una tabla Delta (por ejemplo, porque todavía no existe en S3), igual deja en
    Glue una entrada sin StorageDescriptor ni columnas. Athena no puede leer esa entrada (HIVE_UNSUPPORTED_FORMAT) y,
    como el DDL usa IF NOT EXISTS, los intentos siguientes la dan por buena. Esa entrada se considera "rota".
    """
    if tabla_glue is None:
        return "falta"
    sd = tabla_glue.get("StorageDescriptor") or {}
    if not sd.get("Location") or not sd.get("Columns"):
        return "rota"
    return "ok"


def columnas_glue(tabla_glue):
    """Nombres de columna (en minúscula) que Glue tiene guardados, incluidas las de partición."""
    sd = tabla_glue.get("StorageDescriptor") or {}
    return {c["Name"].lower() for c in sd.get("Columns", []) + tabla_glue.get("PartitionKeys", [])}


def esquemas_delta(spark):
    """{tabla: columnas} de las tablas Delta de Gold que ya existen (para detectar entradas de Glue desactualizadas)."""
    salida = {}
    for t in TABLAS:
        ruta = config.lake("gold", t)
        if DeltaTable.isDeltaTable(spark, ruta):
            salida[t] = {c.lower() for c in spark.read.format("delta").load(ruta).schema.fieldNames()}
    return salida


def _existe_delta(s3, uri):
    """True si en `uri` (s3://bucket/prefijo) ya hay un _delta_log, es decir, si la tabla Delta existe."""
    bucket, prefijo = uri.replace("s3://", "", 1).split("/", 1)
    r = s3.list_objects_v2(Bucket=bucket, Prefix=prefijo.rstrip("/") + "/_delta_log/", MaxKeys=1)
    return r.get("KeyCount", 0) > 0


def _tabla_glue(glue, bd, tabla):
    try:
        return glue.get_table(DatabaseName=bd, Name=tabla)["Table"]
    except glue.exceptions.EntityNotFoundException:
        return None


def _ejecutar_athena(athena, sql, workgroup, espera=1.0, max_espera_s=120):
    """Ejecuta una sentencia en Athena y devuelve (estado, motivo)."""
    q = athena.start_query_execution(QueryString=sql, WorkGroup=workgroup)["QueryExecutionId"]
    inicio = time.time()
    while True:
        estado = athena.get_query_execution(QueryExecutionId=q)["QueryExecution"]["Status"]
        if estado["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return estado["State"], estado.get("StateChangeReason")
        if time.time() - inicio > max_espera_s:
            return "TIMEOUT", "Athena no respondió en %d s" % max_espera_s
        time.sleep(espera)


def registrar_glue(s3=None, glue=None, athena=None, esquemas=None):
    """Registra en Glue (vía Athena) las tablas Delta de Gold para que Athena y Power BI las lean.

    Por cada tabla:
      * si todavía no existe en S3 (el DAG aún no la ha creado), la salta y la registra en una corrida posterior;
      * si ya está bien registrada, no hace nada;
      * si sus columnas en Glue no son las de la tabla Delta (`esquemas`, p. ej. tras agregar columnas a Gold),
        la borra y la vuelve a crear: Athena se niega a leerla con INVALID_GLUE_SCHEMA;
      * si quedó una entrada rota de un intento fallido, la borra y la vuelve a crear;
      * si el registro falla, borra la entrada a medias, sigue con las demás y al final falla con la lista,
        para que Airflow reintente sin dejar el catálogo roto.
    En local (MinIO) no hay Athena: se omite sin error. En AWS falta configuración = error.
    """
    if config.S3_ENDPOINT:
        log.info("registro en Glue omitido (modo local)")
        return {}
    bd, wg = config.env("GLUE_DATABASE"), config.env("ATHENA_WORKGROUP")
    if not bd or not wg:
        raise RuntimeError("Faltan GLUE_DATABASE o ATHENA_WORKGROUP: sin ellas Athena no ve las tablas de Gold")

    if s3 is None or glue is None or athena is None:
        import boto3

        s3 = s3 or boto3.client("s3", region_name=config.REGION)
        glue = glue or boto3.client("glue", region_name=config.REGION)
        athena = athena or boto3.client("athena", region_name=config.REGION)

    resultado, fallas = {}, []
    for t in TABLAS:
        uri = config.lake("gold", t).replace("s3a://", "s3://").rstrip("/")
        if not _existe_delta(s3, uri):
            resultado[t] = "pendiente (aún no existe en S3)"
            log.info("Glue: %s todavía no existe en S3; se registrará en una corrida posterior", t)
            continue
        actual = _tabla_glue(glue, bd, t)
        estado = estado_en_glue(actual)
        if estado == "ok" and esquemas and t in esquemas and columnas_glue(actual) != set(esquemas[t]):
            estado = "desactualizada"
            glue.delete_table(DatabaseName=bd, Name=t)
            log.warning("Glue: %s tenía columnas viejas (%s); se borró para recrearla con el esquema de Delta", t,
                        ", ".join(sorted(set(esquemas[t]) ^ columnas_glue(actual))))
        if estado == "ok":
            resultado[t] = "ya registrada"
            continue
        if estado == "rota":
            glue.delete_table(DatabaseName=bd, Name=t)
            log.warning("Glue: %s tenía una entrada rota de un intento anterior; se borró para recrearla", t)
        ddl = "CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s LOCATION '%s/' TBLPROPERTIES ('table_type'='DELTA')" % (bd, t, uri)
        st, motivo = _ejecutar_athena(athena, ddl, wg)
        if st == "SUCCEEDED" and estado_en_glue(_tabla_glue(glue, bd, t)) == "ok":
            resultado[t] = {"falta": "registrada", "rota": "reparada", "desactualizada": "actualizada"}[estado]
            log.info("Glue: tabla %s %s", t, resultado[t])
            continue
        # Falló: no dejar una entrada a medias que bloquee los reintentos.
        if _tabla_glue(glue, bd, t) is not None and estado_en_glue(_tabla_glue(glue, bd, t)) != "ok":
            glue.delete_table(DatabaseName=bd, Name=t)
        resultado[t] = "falló: %s" % (motivo or st)
        fallas.append(t)
        log.error("Glue: no se pudo registrar %s (%s)", t, motivo or st)

    log.info("Glue: %s", resultado)
    if fallas:
        raise RuntimeError("No se pudieron registrar en Glue: %s (Airflow reintentará)" % ", ".join(fallas))
    return resultado


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
        esquemas = None
        if not config.S3_ENDPOINT:
            spark = get_spark("fraudlens-glue")
            try:
                esquemas = esquemas_delta(spark)
            finally:
                spark.stop()
        registrar_glue(esquemas=esquemas)
    else:
        refrescar_metabase()
    return 0


if __name__ == "__main__":
    sys.exit(main())

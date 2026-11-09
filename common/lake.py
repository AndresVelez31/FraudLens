"""Utilidades de archivos pequeños sobre S3/MinIO o disco local (latido, listado de CSV de landing)."""
import json
import os
import time
from urllib.parse import urlparse

from common import config


def _es_s3(uri):
    return uri.startswith(("s3a://", "s3://"))


def _s3():
    import boto3

    return boto3.client("s3", region_name=config.REGION, endpoint_url=config.S3_ENDPOINT)


def _partir(uri):
    p = urlparse(uri.replace("s3a://", "s3://"))
    return p.netloc, p.path.lstrip("/")


def escribir_texto(uri, texto):
    if _es_s3(uri):
        bucket, clave = _partir(uri)
        _s3().put_object(Bucket=bucket, Key=clave, Body=texto.encode())
    else:
        os.makedirs(os.path.dirname(uri), exist_ok=True)
        with open(uri, "w", encoding="utf-8") as f:
            f.write(texto)


def leer_texto(uri):
    """Devuelve el texto o None si no existe."""
    try:
        if _es_s3(uri):
            bucket, clave = _partir(uri)
            return _s3().get_object(Bucket=bucket, Key=clave)["Body"].read().decode()
        with open(uri, encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None


def listar(prefijo_uri):
    """Lista las URIs de los archivos bajo un prefijo (ordenadas)."""
    if _es_s3(prefijo_uri):
        bucket, clave = _partir(prefijo_uri)
        paginas = _s3().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=clave)
        return sorted("s3a://%s/%s" % (bucket, o["Key"]) for p in paginas for o in p.get("Contents", []))
    salida = []
    for raiz, _, archivos in os.walk(prefijo_uri):
        salida += [os.path.join(raiz, a) for a in archivos]
    return sorted(salida)


def subir_archivo(ruta_local, uri_destino):
    if _es_s3(uri_destino):
        bucket, clave = _partir(uri_destino)
        _s3().upload_file(ruta_local, bucket, clave)
    else:
        os.makedirs(os.path.dirname(uri_destino), exist_ok=True)
        with open(ruta_local, "rb") as o, open(uri_destino, "wb") as d:
            d.write(o.read())


# ---- latido del job de streaming (lo lee el sensor `check_streaming_vivo` de Airflow) ----
def latido_uri():
    return config.lake("_estado", "streaming_latido.json")


def escribir_latido(estado):
    escribir_texto(latido_uri(), json.dumps(dict(estado, ts=time.time())))


def edad_latido():
    """Segundos desde el último latido; None si nunca hubo."""
    txt = leer_texto(latido_uri())
    if not txt:
        return None
    return time.time() - json.loads(txt)["ts"]

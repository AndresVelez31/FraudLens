"""Lambda notificadora de FraudLens (tarea D3): DynamoDB Streams -> un correo resumen por lote -> SNS.

El streaming de Spark solo escribe las alertas en DynamoDB (NOTIFICAR_DESDE=lambda). Cada alerta nueva es un INSERT
en el stream de la tabla `alertas_fraude`. El event source mapping junta hasta 5 minutos de INSERTs
(MaximumBatchingWindowInSeconds=300, solo eventos INSERT por filtro) y llama a esta función una vez por lote:
sale un solo correo con el conteo por regla y las 100 alertas de mayor riesgo (el mismo texto que usaba Spark,
common/resumen.py).

Fallos: si SNS falla, la función lanza el error y Lambda reintenta el lote (MaximumRetryAttempts) partiéndolo en dos
(BisectBatchOnFunctionError). Si aun así falla, el mapping manda la referencia del lote a la cola SQS de fallos
(OnFailure), para revisarlo o reprocesarlo. Con FORZAR_ERROR=1 la función falla a propósito (prueba de ese camino).

Variables: SNS_TOPIC_ARN, DYNAMODB_TABLE (solo para el texto), FORZAR_ERROR.
"""
import json
import logging
import os

import boto3
from boto3.dynamodb.types import TypeDeserializer

try:                       # en el paquete de la Lambda, resumen.py va al lado de este archivo
    import resumen
except ImportError:        # en las pruebas del repositorio
    from common import resumen

log = logging.getLogger()
log.setLevel(logging.INFO)
_deserializar = TypeDeserializer().deserialize
_sns = None


def _cliente_sns():
    global _sns
    if _sns is None:
        _sns = boto3.client("sns")
    return _sns


def a_dict(imagen):
    """NewImage del stream ({"regla": {"S": "R4"}, "monto_total": {"N": "10.5"}, ...}) -> dict plano."""
    salida = {}
    for k, v in (imagen or {}).items():
        valor = _deserializar(v)
        salida[k] = float(valor) if type(valor).__name__ == "Decimal" else valor
    return salida


def alertas_nuevas(evento):
    return [a_dict(r["dynamodb"].get("NewImage")) for r in evento.get("Records", [])
            if r.get("eventName") == "INSERT" and r.get("dynamodb", {}).get("NewImage")]


def lambda_handler(evento, contexto):
    alertas = alertas_nuevas(evento)
    if os.environ.get("FORZAR_ERROR") == "1":
        raise RuntimeError("FORZAR_ERROR=1: fallo a propósito para probar reintentos y la cola SQS de fallos")
    if not alertas:
        return {"alertas": 0, "correos": 0}
    conteo = {}
    for a in alertas:
        conteo[a.get("regla")] = conteo.get(a.get("regla"), 0) + 1
    mejores = sorted(alertas, key=resumen.riesgo)[:resumen.MAX_DETALLE]
    desde = min((a.get("ts_alerta") for a in alertas if a.get("ts_alerta")), default=None)
    asunto, cuerpo = resumen.resumen(conteo, mejores, desde, tabla=os.environ.get("DYNAMODB_TABLE", "alertas_fraude"))
    # Si SNS falla, el error sube: Lambda reintenta el lote y, si sigue fallando, lo deja en la cola de fallos.
    _cliente_sns().publish(TopicArn=os.environ["SNS_TOPIC_ARN"], Subject=asunto, Message=cuerpo)
    log.info(json.dumps({"alertas": len(alertas), "por_regla": conteo, "asunto": asunto}, ensure_ascii=False))
    return {"alertas": len(alertas), "correos": 1}

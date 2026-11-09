"""Piezas comunes de los DAG: argumentos por defecto, comando de los jobs y aviso de fallo por SNS."""
import json
import logging
import os
from datetime import timedelta

log = logging.getLogger("fraudlens.dags")
RAIZ = os.environ.get("FRAUDLENS_HOME", "/opt/fraudlens")


def comando(modulo, *args):
    """Los jobs de Spark corren dentro del worker de Airflow (LocalExecutor), con el código del repositorio."""
    return "cd %s && python -m %s %s" % (RAIZ, modulo, " ".join(args))


def notificar_fallo(contexto):
    """Se llama cuando una tarea agotó sus reintentos: publica en SNS (o deja el aviso en el log si no hay topic)."""
    ti = contexto["task_instance"]
    mensaje = {"dag": ti.dag_id, "tarea": ti.task_id, "intento": ti.try_number, "ejecucion": str(contexto["logical_date"])}
    arn = os.environ.get("SNS_TOPIC_ARN")
    if not arn:
        log.error("FALLO de tarea (sin SNS configurado): %s", mensaje)
        return
    try:
        import boto3

        boto3.client("sns", region_name=os.environ.get("AWS_REGION", "us-east-1")).publish(
            TopicArn=arn, Subject=("FraudLens: falló %s.%s" % (ti.dag_id, ti.task_id))[:100], Message=json.dumps(mensaje))
    except Exception:  # noqa: BLE001
        log.exception("No se pudo avisar el fallo por SNS: %s", mensaje)


ARGS_POR_DEFECTO = {
    "owner": "fraudlens",
    "retries": 3,                           # RNF-05: 3 reintentos
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,      # con espera exponencial
    "max_retry_delay": timedelta(minutes=5),
    "on_failure_callback": notificar_fallo, # y luego notifica
    "depends_on_past": False,
}

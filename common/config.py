"""Configuración central: todo sale de variables de entorno (ver .env.example).

El mismo código corre en tres modos sin cambios:
  * local con docker-compose (MinIO como S3, DynamoDB Local),
  * local sin Docker (LAKE_URI apuntando a una carpeta),
  * AWS Learner Lab (S3, DynamoDB y SNS reales con el LabInstanceProfile).
"""
import os


def env(nombre, defecto=None):
    valor = os.environ.get(nombre)
    return valor if valor not in (None, "") else defecto


REGION = env("AWS_REGION", "us-east-1")

# Raíz del lakehouse: "s3a://bucket" (S3 o MinIO) o una ruta local.
LAKE_URI = env("LAKE_URI") or "s3a://%s" % env("S3_BUCKET", "fraudlens-lake")
S3_ENDPOINT = env("S3_ENDPOINT")  # solo MinIO / pruebas; vacío en AWS

DYNAMODB_TABLE = env("DYNAMODB_TABLE", "alertas_fraude")
DYNAMODB_ENDPOINT = env("DYNAMODB_ENDPOINT")  # DynamoDB Local; vacío en AWS
SNS_TOPIC_ARN = env("SNS_TOPIC_ARN")  # vacío: no se notifica (solo se registra en el log)

KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = env("KAFKA_TOPIC", "transacciones")
KAFKA_DLQ_TOPIC = env("KAFKA_DLQ_TOPIC", "transacciones_dlq")

# Versión de reglas con la que corre el streaming (RF-16: se puede reprocesar con otra).
RULE_VERSION = env("RULE_VERSION", "v1")

# Reglas que corren en el streaming. Con IEEE-CIS se recomienda R2,R3,R4: R1 (ráfaga) casi no acierta con datos
# reales y genera la mayoría de los falsos positivos (docs/evaluacion/README.md, F3).
REGLAS_ACTIVAS = frozenset(r.strip().upper() for r in env("REGLAS_ACTIVAS", "R1,R2,R3,R4").split(",") if r.strip())


def regla_activa(regla):
    return regla.upper() in REGLAS_ACTIVAS

SOURCE_SYSTEM = "fraudlens-app"


def lake(*partes):
    """Ruta dentro del lakehouse: lake('silver', 'transacciones')."""
    return "/".join([LAKE_URI.rstrip("/")] + [p.strip("/") for p in partes])


def pii_key():
    """Clave del HMAC de datos personales (RNF-07). Nunca va en el repositorio."""
    clave = env("PII_HMAC_KEY")
    if not clave:
        raise RuntimeError("Falta PII_HMAC_KEY: defínela en .env (no se sube al repositorio).")
    return clave

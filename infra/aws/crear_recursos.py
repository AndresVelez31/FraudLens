"""Crea (de forma idempotente) los recursos de datos del proyecto.

  AWS Learner Lab:   python infra/aws/crear_recursos.py --todo --email equipo@correo.co
  Local (compose):   python infra/aws/crear_recursos.py --solo s3 dynamodb      (con S3_ENDPOINT y DYNAMODB_ENDPOINT)

Recursos: bucket S3 con cifrado SSE-S3 y acceso público bloqueado, tabla DynamoDB `alertas_fraude` (on-demand, con
índices por cliente y por regla), topic SNS, base de datos de Glue y workgroup de Athena. Todos cobran solo por uso
(ADR-06): no se crea nada que cobre por hora encendido.
"""
import argparse
import os
import sys

import boto3
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_REGION") or "us-east-2"
BUCKET = os.environ.get("S3_BUCKET") or "fraudlens-lake"
TABLA = os.environ.get("DYNAMODB_TABLE") or "alertas_fraude"
GLUE_DB = os.environ.get("GLUE_DATABASE") or "fraudlens"
WORKGROUP = os.environ.get("ATHENA_WORKGROUP") or "fraudlens"
PREFIJOS = ["raw/", "landing/", "bronze/", "silver/", "gold/", "models/", "checkpoints/", "athena-results/", "_estado/"]


def cliente(servicio, endpoint=None):
    # Una variable de endpoint vacía (modo AWS) significa "usar el endpoint real de AWS".
    return boto3.client(servicio, region_name=REGION, endpoint_url=endpoint or None)


def crear_s3():
    s3 = cliente("s3", os.environ.get("S3_ENDPOINT"))
    try:
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=BUCKET)
        else:
            s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION})
        print("S3: bucket creado", BUCKET)
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise
        print("S3: el bucket ya existe", BUCKET)
    if not os.environ.get("S3_ENDPOINT"):  # estas dos llamadas no aplican a MinIO
        s3.put_bucket_encryption(
            Bucket=BUCKET,
            ServerSideEncryptionConfiguration={"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]},
        )
        s3.put_public_access_block(
            Bucket=BUCKET,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True,
            },
        )
    for p in PREFIJOS:
        s3.put_object(Bucket=BUCKET, Key=p, Body=b"")


def crear_dynamodb():
    ddb = cliente("dynamodb", os.environ.get("DYNAMODB_ENDPOINT"))
    try:
        ddb.describe_table(TableName=TABLA)
        print("DynamoDB: la tabla ya existe", TABLA)
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    # Clave de partición = id_alerta determinista: escribir dos veces la misma alerta cae sobre el mismo ítem
    # (idempotencia). Las consultas de la API van por los índices secundarios.
    ddb.create_table(
        TableName=TABLA,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "id_alerta", "AttributeType": "S"},
            {"AttributeName": "id_cliente", "AttributeType": "S"},
            {"AttributeName": "regla", "AttributeType": "S"},
            {"AttributeName": "ts_alerta", "AttributeType": "S"},
        ],
        KeySchema=[{"AttributeName": "id_alerta", "KeyType": "HASH"}],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "por_cliente",
                "KeySchema": [{"AttributeName": "id_cliente", "KeyType": "HASH"}, {"AttributeName": "ts_alerta", "KeyType": "RANGE"}],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "por_regla",
                "KeySchema": [{"AttributeName": "regla", "KeyType": "HASH"}, {"AttributeName": "ts_alerta", "KeyType": "RANGE"}],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
    )
    ddb.get_waiter("table_exists").wait(TableName=TABLA)
    print("DynamoDB: tabla creada", TABLA)


def crear_sns(email=None):
    sns = cliente("sns")
    arn = sns.create_topic(Name="alertas-fraude")["TopicArn"]  # idempotente
    if email:
        sns.subscribe(TopicArn=arn, Protocol="email", Endpoint=email)
        print("SNS: suscripción enviada a %s (hay que confirmarla desde el correo)" % email)
    print("SNS: SNS_TOPIC_ARN=%s" % arn)


def crear_glue_athena():
    glue = cliente("glue")
    try:
        glue.create_database(DatabaseInput={"Name": GLUE_DB})
        print("Glue: base de datos creada", GLUE_DB)
    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise
        print("Glue: la base de datos ya existe", GLUE_DB)
    athena = cliente("athena")
    try:
        athena.create_work_group(
            Name=WORKGROUP,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://%s/athena-results/" % BUCKET}},
        )
        print("Athena: workgroup creado", WORKGROUP)
    except ClientError as e:
        if e.response["Error"]["Code"] != "InvalidRequestException":  # ya existe
            raise
        print("Athena: el workgroup ya existe", WORKGROUP)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--todo", action="store_true")
    ap.add_argument("--solo", nargs="+", choices=["s3", "dynamodb", "sns", "glue"], default=[])
    ap.add_argument("--email", help="correo para suscribir al topic de SNS")
    a = ap.parse_args()
    if not a.todo and not a.solo:
        ap.error("indica --todo o --solo ...")
    pasos = a.solo or ["s3", "dynamodb", "sns", "glue"]
    if "s3" in pasos:
        crear_s3()
    if "dynamodb" in pasos:
        crear_dynamodb()
    if "sns" in pasos:
        crear_sns(a.email)
    if "glue" in pasos:
        crear_glue_athena()
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env bash
# Despliega la Lambda notificadora de FraudLens (tarea D3). Idempotente: se puede correr varias veces.
#
#   DynamoDB alertas_fraude (stream NEW_IMAGE)
#      -> event source mapping (solo INSERT, lotes de hasta 5 min, 2 reintentos, parte el lote si falla)
#      -> Lambda fraudlens-notificador (Python 3.12) -> SNS alertas-fraude (un correo por lote)
#      -> si falla después de los reintentos: cola SQS fraudlens-notificador-fallos
#
# Uso (con la sesión de AWS del proyecto; todo en la región del proyecto):
#   AWS_PROFILE=fraudlens bash infra/aws/notificador.sh            # crea o actualiza todo
#   AWS_PROFILE=fraudlens bash infra/aws/notificador.sh --forzar-error 1   # prueba del camino de fallos
#   AWS_PROFILE=fraudlens bash infra/aws/notificador.sh --forzar-error 0   # vuelve a la normalidad
# Después, en las EC2: NOTIFICAR_DESDE=lambda en .env para que Spark deje de mandar correos.
set -euo pipefail

REGION="${AWS_REGION:-us-east-2}"
TABLA="${DYNAMODB_TABLE:-alertas_fraude}"
FUNCION="fraudlens-notificador"
ROL="fraudlens-notificador"
COLA="fraudlens-notificador-fallos"
TOPICO="${SNS_TOPICO:-alertas-fraude}"
VENTANA_S="${VENTANA_S:-300}"
RAIZ="$(cd "$(dirname "$0")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
TMPW="$(cd "$TMP" && (pwd -W 2>/dev/null || pwd))"   # el AWS CLI de Windows no lee /tmp de Git Bash
aws() { command aws --region "$REGION" "$@"; }

if [ "${1:-}" = "--forzar-error" ]; then
  VARS="$(aws lambda get-function-configuration --function-name "$FUNCION" --query Environment.Variables --output json)"
  NUEVAS="$(python -c "import json,sys; v=json.loads(sys.argv[1]); v['FORZAR_ERROR']=sys.argv[2]; print(json.dumps({'Variables': v}))" "$VARS" "$2")"
  aws lambda update-function-configuration --function-name "$FUNCION" --environment "$NUEVAS" --query LastUpdateStatus --output text
  exit 0
fi

CUENTA="$(aws sts get-caller-identity --query Account --output text)"
TOPICO_ARN="arn:aws:sns:$REGION:$CUENTA:$TOPICO"
aws sns get-topic-attributes --topic-arn "$TOPICO_ARN" >/dev/null   # falla aquí si el topic no existe

echo "1) Stream de DynamoDB en $TABLA"
STREAM="$(aws dynamodb describe-table --table-name "$TABLA" --query Table.LatestStreamArn --output text)"
if [ "$STREAM" = "None" ] || [ -z "$STREAM" ]; then
  aws dynamodb update-table --table-name "$TABLA" --stream-specification StreamEnabled=true,StreamViewType=NEW_IMAGE >/dev/null
  aws dynamodb wait table-exists --table-name "$TABLA"
  STREAM="$(aws dynamodb describe-table --table-name "$TABLA" --query Table.LatestStreamArn --output text)"
fi
echo "   $STREAM"

echo "2) Cola SQS de fallos: $COLA"
COLA_URL="$(aws sqs create-queue --queue-name "$COLA" --attributes MessageRetentionPeriod=1209600 --query QueueUrl --output text)"
COLA_ARN="$(aws sqs get-queue-attributes --queue-url "$COLA_URL" --attribute-names QueueArn --query Attributes.QueueArn --output text)"

echo "3) Rol de la Lambda: $ROL"
cat > "$TMP/confianza.json" <<'JSON'
{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"},
  "Action": "sts:AssumeRole"}]}
JSON
if ! aws iam get-role --role-name "$ROL" >/dev/null 2>&1; then
  aws iam create-role --role-name "$ROL" --assume-role-policy-document "file://$TMPW/confianza.json" \
    --description "FraudLens: Lambda notificadora (D3)" >/dev/null
fi
aws iam attach-role-policy --role-name "$ROL" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaDynamoDBExecutionRole
cat > "$TMP/permisos.json" <<JSON
{"Version": "2012-10-17", "Statement": [
  {"Effect": "Allow", "Action": "sns:Publish", "Resource": "$TOPICO_ARN"},
  {"Effect": "Allow", "Action": "sqs:SendMessage", "Resource": "$COLA_ARN"}]}
JSON
aws iam put-role-policy --role-name "$ROL" --policy-name sns-y-cola-de-fallos --policy-document "file://$TMPW/permisos.json"
ROL_ARN="$(aws iam get-role --role-name "$ROL" --query Role.Arn --output text)"

echo "4) Paquete y función: $FUNCION"
cp "$RAIZ/lambdas/notificador/lambda_function.py" "$RAIZ/common/resumen.py" "$TMP/"
python - "$TMP" <<'PY'
import os, sys, zipfile
d = sys.argv[1]
with zipfile.ZipFile(os.path.join(d, "paquete.zip"), "w", zipfile.ZIP_DEFLATED) as z:
    for f in ("lambda_function.py", "resumen.py"):
        z.write(os.path.join(d, f), f)
PY
ZIP="fileb://$TMPW/paquete.zip"
VARS="Variables={SNS_TOPIC_ARN=$TOPICO_ARN,DYNAMODB_TABLE=$TABLA,FORZAR_ERROR=0}"
if aws lambda get-function --function-name "$FUNCION" >/dev/null 2>&1; then
  aws lambda update-function-code --function-name "$FUNCION" --zip-file "$ZIP" --query LastUpdateStatus --output text
  aws lambda wait function-updated --function-name "$FUNCION"
  aws lambda update-function-configuration --function-name "$FUNCION" --environment "$VARS" --query LastUpdateStatus --output text
else
  for i in 1 2 3 4 5 6; do   # el rol recién creado tarda unos segundos en poder asumirse
    aws lambda create-function --function-name "$FUNCION" --runtime python3.12 --handler lambda_function.lambda_handler \
      --role "$ROL_ARN" --zip-file "$ZIP" --timeout 60 --memory-size 256 --environment "$VARS" \
      --description "FraudLens D3: resumen de alertas nuevas (stream de DynamoDB) por SNS" \
      --query State --output text && break
    sleep 10
  done
fi
aws lambda wait function-active-v2 --function-name "$FUNCION"

echo "5) Event source mapping (stream -> Lambda)"
MAPEO="$(aws lambda list-event-source-mappings --function-name "$FUNCION" --event-source-arn "$STREAM" \
  --query 'EventSourceMappings[0].UUID' --output text)"
cat > "$TMP/filtro.json" <<'JSON'
{"Filters": [{"Pattern": "{\"eventName\": [\"INSERT\"]}"}]}
JSON
COMUNES=(--batch-size 1000 --maximum-batching-window-in-seconds "$VENTANA_S" --maximum-retry-attempts 2
         --bisect-batch-on-function-error --maximum-record-age-in-seconds 3600
         --destination-config "OnFailure={Destination=$COLA_ARN}" --filter-criteria "file://$TMPW/filtro.json")
if [ "$MAPEO" = "None" ] || [ -z "$MAPEO" ]; then
  aws lambda create-event-source-mapping --function-name "$FUNCION" --event-source-arn "$STREAM" \
    --starting-position LATEST "${COMUNES[@]}" --query State --output text
else
  aws lambda update-event-source-mapping --uuid "$MAPEO" "${COMUNES[@]}" --query State --output text
fi

echo "Listo. Función $FUNCION, stream $STREAM, cola de fallos $COLA_URL"
echo "Ahora, en las EC2: NOTIFICAR_DESDE=lambda en .env y reiniciar el streaming."

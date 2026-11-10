"""Lambda notificadora (D3): un lote del stream de DynamoDB -> un solo correo por SNS. Usa moto."""
import importlib.util
import json
import os

import pytest

from common import resumen

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def cargar_lambda():
    spec = importlib.util.spec_from_file_location(
        "lambda_function", os.path.join(RAIZ, "lambdas", "notificador", "lambda_function.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def registro(i, regla="R4", puntaje=0.3, evento="INSERT"):
    imagen = {"id_alerta": {"S": "a%d" % i}, "regla": {"S": regla}, "id_cliente": {"S": "C%d" % i},
              "id_transaccion": {"S": "T%d" % i}, "id_comercio": {"S": "W"}, "monto_total": {"N": "%d.5" % i},
              "ts_evento": {"S": "2026-05-02T00:00:00.000Z"}, "ts_alerta": {"S": "2026-11-10T03:%02d:00.000Z" % (i % 60)},
              "detalle": {"S": json.dumps({"puntaje": puntaje, "umbral": 0.155})}}
    return {"eventName": evento, "dynamodb": {"NewImage": imagen}}


@pytest.fixture
def sns(monkeypatch):
    moto = pytest.importorskip("moto")
    import boto3
    from moto.core import DEFAULT_ACCOUNT_ID
    from moto.sns import sns_backends

    for k, v in {"AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t", "AWS_DEFAULT_REGION": "us-east-2"}.items():
        monkeypatch.setenv(k, v)
    with moto.mock_aws():
        arn = boto3.client("sns", region_name="us-east-2").create_topic(Name="alertas-fraude")["TopicArn"]
        monkeypatch.setenv("SNS_TOPIC_ARN", arn)
        yield lambda: sns_backends[DEFAULT_ACCOUNT_ID]["us-east-2"].topics[arn].sent_notifications


def test_un_lote_de_300_alertas_manda_un_solo_correo(sns):
    lam = cargar_lambda()
    evento = {"Records": [registro(i, puntaje=i / 1000) for i in range(300)] + [registro(999, evento="MODIFY")]}
    assert lam.lambda_handler(evento, None) == {"alertas": 300, "correos": 1}   # el MODIFY no cuenta
    enviados = sns()
    assert len(enviados) == 1
    _, cuerpo, asunto = enviados[0][:3]
    assert asunto == "FraudLens: 300 alertas nuevas (R4: 300)"
    filas = [l for l in cuerpo.splitlines() if l.startswith("- ")]
    assert len(filas) == 100 and filas[0].endswith("| 0.30") and "... y 200 más." in cuerpo
    assert "desde 2026-11-09 22:00:00 (hora de Colombia)" in cuerpo                 # 03:00Z = 22:00 en Colombia


def test_lote_sin_inserciones_no_manda_correo(sns):
    lam = cargar_lambda()
    assert lam.lambda_handler({"Records": [registro(1, evento="MODIFY")]}, None) == {"alertas": 0, "correos": 0}
    assert sns() == []


def test_error_forzado_sube_para_que_lambda_reintente(sns, monkeypatch):
    lam = cargar_lambda()
    monkeypatch.setenv("FORZAR_ERROR", "1")
    with pytest.raises(RuntimeError):
        lam.lambda_handler({"Records": [registro(1)]}, None)
    assert sns() == []


def test_hora_colombia():
    assert resumen.hora_colombia("2026-11-10T03:42:34.738Z") == "2026-11-09 22:42:34 (hora de Colombia)"
    assert resumen.hora_colombia("no es fecha") == "no es fecha"

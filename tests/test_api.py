"""Pruebas de la API de alertas (RF-17) contra un DynamoDB simulado en memoria con moto."""
import importlib

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws

ALERTAS = [
    # id_alerta, id_cliente, regla, ts_alerta
    ("a1", "C1", "R1", "2026-11-04T10:00:00.000Z"),
    ("a2", "C1", "R2", "2026-11-04T11:00:00.000Z"),
    ("a3", "C1", "R1", "2026-11-04T12:00:00.000Z"),
    ("a4", "C2", "R3", "2026-11-04T13:00:00.000Z"),
    ("a5", "C2", "R1", "2026-11-04T14:00:00.000Z"),
]


@pytest.fixture
def cliente(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t", "AWS_REGION": "us-east-1",
                 "AWS_DEFAULT_REGION": "us-east-1", "DYNAMODB_TABLE": "alertas_fraude"}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("DYNAMODB_ENDPOINT", raising=False)
    with mock_aws():
        from infra.aws import crear_recursos

        importlib.reload(crear_recursos)
        crear_recursos.crear_dynamodb()
        t = boto3.resource("dynamodb", region_name="us-east-1").Table("alertas_fraude")
        for id_, cli, regla, ts in ALERTAS:
            t.put_item(Item={"id_alerta": id_, "id_cliente": cli, "regla": regla, "ts_alerta": ts, "monto_total": 1500})
        import api.main as main

        importlib.reload(main)
        yield TestClient(main.app)


def ids(resp):
    return [a["id_alerta"] for a in resp.json()["alertas"]]


def test_por_cliente_mas_recientes_primero(cliente):
    r = cliente.get("/alertas", params={"id_cliente": "C1"})
    assert r.status_code == 200 and ids(r) == ["a3", "a2", "a1"]


def test_por_regla(cliente):
    assert sorted(ids(cliente.get("/alertas", params={"regla": "R1"}))) == ["a1", "a3", "a5"]


def test_por_cliente_y_regla(cliente):
    assert ids(cliente.get("/alertas", params={"id_cliente": "C1", "regla": "R1"})) == ["a3", "a1"]


def test_rango_de_tiempo(cliente):
    r = cliente.get("/alertas", params={"id_cliente": "C1", "desde": "2026-11-04T10:30:00.000Z", "hasta": "2026-11-04T11:30:00.000Z"})
    assert ids(r) == ["a2"]
    assert ids(cliente.get("/alertas", params={"regla": "R1", "desde": "2026-11-04T13:00:00.000Z"})) == ["a5"]


def test_limite_y_numeros_como_json(cliente):
    r = cliente.get("/alertas", params={"id_cliente": "C1", "limite": 2})
    assert len(r.json()["alertas"]) == 2 and r.json()["alertas"][0]["monto_total"] == 1500.0


def test_sin_filtros_devuelve_las_ultimas(cliente):
    assert ids(cliente.get("/alertas", params={"limite": 3})) == ["a5", "a4", "a3"]


def test_regla_invalida_es_422_y_salud_ok(cliente):
    assert cliente.get("/alertas", params={"regla": "R9"}).status_code == 422
    assert cliente.get("/salud").json()["estado"] == "ok"

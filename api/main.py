"""API REST de alertas (RF-17): lee la tabla `alertas_fraude` de DynamoDB.

  GET /alertas?id_cliente=C0000123&regla=R1&desde=2026-11-04T00:00:00Z&hasta=2026-11-05T00:00:00Z&limite=50
  GET /salud

Las consultas usan los índices secundarios (`por_cliente`, `por_regla`): nunca se recorre la tabla completa salvo
que no se envíe ningún filtro (en ese caso se devuelven las últimas alertas con un límite).
Las lecturas son eventualmente consistentes (AP, ADR-04): una alerta puede tardar segundos en aparecer.
"""
import os
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key
from fastapi import FastAPI, HTTPException, Query

TABLA = os.environ.get("DYNAMODB_TABLE", "alertas_fraude")
REGION = os.environ.get("AWS_REGION", "us-east-1")
ENDPOINT = os.environ.get("DYNAMODB_ENDPOINT") or None
LIMITE_MAXIMO = 500

app = FastAPI(title="FraudLens: API de alertas", version="1.0")


def tabla():
    return boto3.resource("dynamodb", region_name=REGION, endpoint_url=ENDPOINT).Table(TABLA)


def _limpio(item):
    return {k: (float(v) if isinstance(v, Decimal) else v) for k, v in item.items()}


def _rango(desde, hasta):
    if desde and hasta:
        return "between", (desde, hasta)
    if desde:
        return "gte", desde
    if hasta:
        return "lte", hasta
    return None, None


def consultar(id_cliente=None, regla=None, desde=None, hasta=None, limite=50):
    t = tabla()
    kw = {"ScanIndexForward": False, "Limit": limite}  # más recientes primero
    if id_cliente or regla:
        indice, clave, valor = ("por_cliente", "id_cliente", id_cliente) if id_cliente else ("por_regla", "regla", regla)
        cond = Key(clave).eq(valor)
        op, arg = _rango(desde, hasta)
        if op == "between":
            cond = cond & Key("ts_alerta").between(*arg)
        elif op == "gte":
            cond = cond & Key("ts_alerta").gte(arg)
        elif op == "lte":
            cond = cond & Key("ts_alerta").lte(arg)
        kw.update(IndexName=indice, KeyConditionExpression=cond)
        if id_cliente and regla:  # el índice es por cliente: la regla se filtra después
            kw["FilterExpression"] = Attr("regla").eq(regla)
            kw.pop("Limit")
        items = []
        while True:
            r = t.query(**kw)
            items += r["Items"]
            if "LastEvaluatedKey" not in r or len(items) >= limite:
                return items[:limite]
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    r = t.scan(Limit=max(limite, 1000))
    items = sorted(r["Items"], key=lambda i: i.get("ts_alerta", ""), reverse=True)
    op, arg = _rango(desde, hasta)
    if op == "between":
        items = [i for i in items if arg[0] <= i["ts_alerta"] <= arg[1]]
    elif op == "gte":
        items = [i for i in items if i["ts_alerta"] >= arg]
    elif op == "lte":
        items = [i for i in items if i["ts_alerta"] <= arg]
    return items[:limite]


@app.get("/alertas")
def alertas(
    id_cliente: str | None = Query(None, description="Cliente, p. ej. C0000123"),
    regla: str | None = Query(None, pattern="^R[1-4]$", description="R1, R2, R3 o R4 (modelo)"),
    desde: str | None = Query(None, description="ISO 8601 UTC, p. ej. 2026-11-04T00:00:00.000Z"),
    hasta: str | None = Query(None, description="ISO 8601 UTC"),
    limite: int = Query(50, ge=1, le=LIMITE_MAXIMO),
):
    try:
        items = consultar(id_cliente, regla, desde, hasta, limite)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=503, detail="No se pudo consultar DynamoDB: %s" % e) from e
    return {"total": len(items), "alertas": [_limpio(i) for i in items]}


@app.get("/salud")
def salud():
    try:
        tabla().load()
        return {"estado": "ok", "tabla": TABLA}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=str(e)) from e

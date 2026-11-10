"""Resumen de alertas por SNS (common/alerts.py): un correo por intervalo, no uno por alerta."""
import json

import pytest

from common import alerts, config


@pytest.fixture
def enviados(monkeypatch):
    """Reemplaza el envío real y deja el estado del módulo limpio en cada prueba."""
    salida = []
    monkeypatch.setattr(alerts, "_enviar", lambda asunto, cuerpo: salida.append((asunto, cuerpo)))
    monkeypatch.setenv("NOTIFICAR_CADA_SEGUNDOS", "300")
    alerts._pendiente.update(conteo={}, alertas=[], primera=None)
    alerts._ultimo_envio[0] = None
    return salida


def _r4(i, puntaje):
    return {"id_alerta": "a%d" % i, "regla": "R4", "id_cliente": "C%d" % i, "id_transaccion": "T%d" % i,
            "id_comercio": "W", "monto_total": 10.0, "ts_evento": "2026-05-02T00:00:00.000Z", "ts_alerta": "2026-11-10T00:00:00.000Z",
            "detalle": json.dumps({"puntaje": puntaje, "umbral": 0.155})}


def test_una_rafaga_de_544_alertas_manda_un_solo_correo_por_intervalo(enviados):
    lotes = [[_r4(i * 100 + j, 0.2) for j in range(68)] for i in range(8)]   # 544 alertas en 8 lotes
    for k, lote in enumerate(lotes):
        alerts.notificar(lote, ahora=1000 + 5 * k)                            # un lote cada 5 s
    assert len(enviados) == 1 and "1 alerta" not in enviados[0][0]            # el primero sale de inmediato
    assert alerts.enviar_pendientes(ahora=1100) == 0                          # aún dentro del intervalo
    assert alerts.enviar_pendientes(ahora=1301) == 544 - 68                   # pasado el intervalo, el resto
    assert len(enviados) == 2 and "476 alertas nuevas (R4: 476)" in enviados[1][0]
    assert alerts.enviar_pendientes(ahora=2000) == 0                          # nada pendiente: no hay correo


def test_el_resumen_lista_primero_las_de_mayor_riesgo(enviados):
    nuevas = [_r4(1, 0.30), _r4(2, 0.90), _r4(3, 0.50),
              {"id_alerta": "b", "regla": "R3", "id_cliente": "C9", "id_transaccion": "T9", "monto_total": 999.0,
               "ts_evento": "2026-05-02T00:00:00.000Z", "detalle": "{}"}]
    alerts.notificar(nuevas, ahora=0)
    asunto, cuerpo = enviados[0]
    assert asunto == "FraudLens: 4 alertas nuevas (R3: 1, R4: 3)"
    filas = [l for l in cuerpo.splitlines() if l.startswith("- ")]
    assert [f.split("|")[2].split()[-1] for f in filas] == ["T2", "T3", "T1", "T9"]   # R4 por puntaje, luego el resto
    assert filas[0].endswith("| 0.90") and "| W |" not in filas[3]   # R3 sin tipo: "-"
    assert "GET /alertas?regla=R4&desde=2026-11-10T00:00:00.000Z" in cuerpo


def test_con_mas_de_100_dice_cuantas_faltan(enviados):
    alerts.notificar([_r4(i, i / 1000) for i in range(130)], ahora=0)
    asunto, cuerpo = enviados[0]
    assert len([l for l in cuerpo.splitlines() if l.startswith("- ")]) == 100
    assert "Las 100 de mayor riesgo (de 130)" in cuerpo and "... y 30 más." in cuerpo
    assert len(cuerpo.encode()) < 256 * 1024


def test_sin_sns_configurado_no_falla(monkeypatch):
    monkeypatch.setattr(config, "SNS_TOPIC_ARN", None)
    alerts._pendiente.update(conteo={}, alertas=[], primera=None)
    alerts._ultimo_envio[0] = None
    assert alerts.notificar([_r4(1, 0.5)], ahora=0) == 1


def test_con_sns_real_simulado_se_publica_un_solo_mensaje(monkeypatch):
    moto = pytest.importorskip("moto")
    import boto3
    from moto.core import DEFAULT_ACCOUNT_ID
    from moto.sns import sns_backends

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "prueba")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "prueba")
    with moto.mock_aws():
        arn = boto3.client("sns", region_name=config.REGION).create_topic(Name="alertas-fraude")["TopicArn"]
        monkeypatch.setattr(config, "SNS_TOPIC_ARN", arn)
        alerts._pendiente.update(conteo={}, alertas=[], primera=None)
        alerts._ultimo_envio[0] = None
        alerts.notificar([_r4(i, 0.3) for i in range(120)], ahora=0)
        enviados = sns_backends[DEFAULT_ACCOUNT_ID][config.REGION].topics[arn].sent_notifications
        assert len(enviados) == 1 and "120 alertas nuevas" in enviados[0][2]   # (id, mensaje, asunto, ...)

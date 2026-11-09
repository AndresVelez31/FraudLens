"""Pruebas del replayer de IEEE-CIS (sin Kafka)."""
import json
import random

from producer import replayer

FILA = {"transaction_dt": "86400", "particion": "prueba", "id_transaccion": "T1", "id_cliente": "U1_315_10",
        "id_comercio": "C", "id_dispositivo": "", "monto": "75.5", "tipo": "C", "region": "R315", "correo_dominio": "mail.com"}


def test_evento_cumple_el_contrato_y_no_lleva_la_etiqueta():
    e = replayer.a_evento(FILA)
    assert e["ts_evento"] == "2026-05-02T00:00:00.000Z"          # FECHA_BASE + 1 día
    assert e["id_dispositivo"] is None and e["monto"] == 75.5 and e["tipo"] == "C"
    assert "isFraud" not in e and "ts_emision" in e and e["correo_dominio"] == "mail.com"


def test_registros_danados_van_a_dlq_o_cuarentena():
    e = replayer.a_evento(FILA)
    tipos = set()
    for s in range(30):
        texto, tipo = replayer.danar(e, random.Random(s))
        tipos.add(tipo)
        if tipo == "cuarentena":
            malo = json.loads(texto)
            assert malo["monto"] < 0 or malo["tipo"] == "Z" or malo["id_cliente"] is None
        else:
            try:
                json.loads(texto)
                raise AssertionError("un registro ilegible no debe ser JSON válido")
            except json.JSONDecodeError:
                pass
    assert tipos == {"cuarentena", "ilegible"}

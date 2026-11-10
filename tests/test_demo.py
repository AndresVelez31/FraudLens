"""La demo reserva exactamente los pagos que el replayer envía con --ultimos."""
from batch import demo
from producer import replayer


def test_reservar_y_ultimos_eligen_los_mismos_pagos():
    filas = [{"id_transaccion": "T%d" % i, "particion": "prueba" if i % 3 else "entrenamiento"} for i in range(30)]
    reservados = demo.ultimos_ids(filas, 4)
    enviados = [f["id_transaccion"] for f in replayer.filas_a_enviar(filas, "prueba", 4)]
    assert reservados == enviados == ["T25", "T26", "T28", "T29"]

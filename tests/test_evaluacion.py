"""Pruebas de las métricas de evaluación contra etiquetas (batch/evaluacion.py)."""
from datetime import datetime, timedelta

from batch import evaluacion
from common import rules


def test_metricas_matriz_de_confusion(spark):
    tx = spark.createDataFrame([("A", 1, 100.0), ("B", 1, 50.0), ("C", 0, 10.0), ("D", 0, 10.0)],
                               "id_transaccion string, is_fraud int, monto double")
    marcadas = spark.createDataFrame([("A",), ("C",)], "id_transaccion string")
    m = evaluacion.metricas(tx, marcadas)
    assert (m["tp"], m["fp"], m["fn"], m["tn"]) == (1, 1, 1, 1)
    assert m["precision"] == 0.5 and m["recall"] == 0.5 and m["f1"] == 0.5
    assert m["alertas_por_mil"] == 500.0 and m["monto_fraude_detectado_pct"] == round(100 * 100 / 150, 2)


def test_r1_marca_las_transacciones_de_la_ventana_que_alerto(spark):
    t0 = datetime(2026, 9, 20, 12, 0, 0)
    filas = [("R%d" % i, "C1", t0 + timedelta(seconds=30 * i), 10.0) for i in range(3)]   # 3 en 1 minuto
    filas += [("N1", "C2", t0, 10.0)]                                                       # cliente tranquilo
    tx = spark.createDataFrame(filas, "id_transaccion string, id_cliente string, ts_evento timestamp, monto double")
    agregado = rules.r1_agregar(tx)
    marcadas = {r.id_transaccion for r in evaluacion.marcadas_r1(tx, agregado, "ieee").collect()}
    assert marcadas == {"R0", "R1", "R2"}   # con la versión calibrada, 3 o más en 5 min alerta
    assert evaluacion.marcadas_r1(tx, agregado, "v1").count() == 0   # con la del E1 no

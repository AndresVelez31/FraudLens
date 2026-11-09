"""Pruebas de las combinaciones de reglas y modelo (ml/comparacion_final.py). Corren en la imagen fraudlens-ml."""
import pytest

np = pytest.importorskip("numpy")
pytest.importorskip("pandas")

from ml import comparacion_final as cf  # noqa: E402


def test_top_k_toma_los_de_mayor_puntaje():
    assert cf.top_k(np.array([0.1, 0.9, 0.5, 0.7]), 2).tolist() == [0, 1, 0, 1]


def test_filtrar_conserva_la_fraccion_de_alertas_con_mayor_puntaje():
    reglas = np.array([1, 1, 1, 1, 0])
    puntaje = np.array([0.2, 0.8, 0.4, 0.6, 0.99])   # el último tiene el mayor puntaje pero ninguna regla lo marcó
    assert cf.filtrar_por_puntaje(reglas, puntaje, 0.5).tolist() == [0, 1, 0, 1, 0]


def test_hibrido_llena_el_presupuesto_sin_quitar_alertas_de_reglas():
    reglas = np.array([1, 0, 0, 0])
    puntaje = np.array([0.0, 0.3, 0.9, 0.5])
    alertas = cf.hibrido(reglas, puntaje, 3)
    assert alertas.tolist() == [1, 0, 1, 1] and alertas.sum() == 3


def test_metricas():
    y = np.array([1, 1, 0, 0])
    m = cf.metricas(y, np.array([100.0, 50.0, 10.0, 10.0]), np.array([1, 0, 1, 0]))
    assert (m["tp"], m["fp"], m["fn"]) == (1, 1, 1)
    assert m["precision"] == 0.5 and m["recall"] == 0.5
    assert m["falsos_positivos_por_mil_legitimas"] == 500.0
    assert m["monto_fraude_detectado_pct"] == round(100 * 100 / 150, 2)

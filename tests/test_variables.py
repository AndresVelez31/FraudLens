"""Pruebas de las variables del modelo R4 (common/variables.py). Corren en la imagen fraudlens-ml."""
import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")

from common import variables  # noqa: E402


def _eventos(filas):
    return pd.DataFrame(filas, columns=["id_transaccion", "t", "id_cliente", "id_dispositivo", "monto", "tipo",
                                        "region", "correo_dominio"])


def test_variables_as_of_de_un_cliente():
    ev = _eventos([
        ("T1000001", 0, "C1", None, 10.0, "W", "R1", "gmail.com"),
        ("T1000002", 60, "C1", "D1", 30.0, "W", "R1", "gmail.com"),
        ("T1000003", 120, "C1", "D1", 100.0, "C", "R1", ""),
        ("T1000004", 4000, "C1", "D2", 20.0, "W", "R1", None),
    ])
    x = variables.calcular(ev)
    assert list(x.columns) == variables.COLUMNAS
    assert x["cli_tx_5min"].tolist() == [1, 2, 3, 1]
    assert x["cli_tx_1h"].tolist() == [1, 2, 3, 1]          # la de t=4000 solo ve (400, 4000]: ella misma
    assert x["cli_tx_previas"].tolist() == [0, 1, 2, 3]
    assert x["cli_seg_desde_anterior"].tolist()[1:] == [60, 60, 3880]
    assert x["cli_monto_prom_30d"].tolist()[1:] == [10.0, 20.0, pytest.approx(140 / 3)]   # promedio previo
    assert x["disp_nuevo_para_cliente"].tolist() == [0, 1, 0, 1]   # la primera vez de D1 y de D2
    assert x["disp_tx_previas"].iloc[2] == 1 and np.isnan(x["disp_tx_previas"].iloc[0])
    assert x["correo_dominio"].isna().tolist() == [False, False, True, True]   # vacío y nulo son lo mismo


def test_streaming_igual_que_entrenamiento():
    """Calcular sobre la historia de Silver + el lote (streaming) da lo mismo que sobre todo el conjunto
    (entrenamiento), para las filas del lote."""
    rng = np.random.default_rng(7)
    n = 400
    ev = pd.DataFrame({
        "id_transaccion": ["T%07d" % (1000000 + i) for i in range(n)],
        "t": np.sort(rng.integers(0, 3 * 86400, n)).astype(float),
        "id_cliente": rng.choice(["C%d" % i for i in range(25)], n),
        "id_dispositivo": rng.choice(["D1", "D2", "D3", None, None], n),
        "monto": rng.uniform(1, 500, n).round(2),
        "tipo": rng.choice(["W", "C", "H"], n), "region": rng.choice(["R1", "R2"], n),
        "correo_dominio": rng.choice(["gmail.com", "mail.com", None], n),
    })
    completo = variables.calcular(ev)

    lote = ev.iloc[-30:]
    clientes, disp = set(lote["id_cliente"]), set(lote["id_dispositivo"].dropna())
    previas = ev.iloc[:-30]
    historia = previas[previas["id_cliente"].isin(clientes) | previas["id_dispositivo"].isin(disp)]
    parcial = variables.calcular(pd.concat([historia, lote], ignore_index=True)).iloc[-30:]

    a = completo.iloc[-30:].reset_index(drop=True)
    b = parcial.reset_index(drop=True)
    for c in variables.COLUMNAS:
        if c in variables.CATEGORICAS:
            assert a[c].astype(object).equals(b[c].astype(object)), c
        else:
            np.testing.assert_allclose(a[c].astype(float), b[c].astype(float), equal_nan=True, err_msg=c)


def test_segundos_desde_base():
    s = variables.segundos_desde_base(pd.Series(["2026-05-02T00:00:01Z"]))
    assert s.tolist() == [86401.0]

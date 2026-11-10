"""Variables del modelo R4 ("contrato"): un solo código para entrenar (ml/modelo_lightgbm.py) y para puntuar en el
streaming (streaming/job.py). Si las dos partes calcularan las variables por separado, cualquier diferencia haría que
el modelo viera en producción algo distinto de lo que vio al entrenar.

Entrada: un DataFrame de pandas con la historia de eventos, con los campos del contrato del evento:
    id_transaccion, t (segundos desde FECHA_BASE), id_cliente, id_dispositivo, monto, tipo, region, correo_dominio
Cada variable se calcula "as-of": para cada fila solo mira filas anteriores (o del mismo instante ya ordenadas antes),
nunca el futuro. En el streaming, la historia es lo que ya hay en Silver para los clientes y dispositivos del lote.

Solo usa pandas y numpy (nada de Spark), para correr igual en las dos imágenes.
"""
from datetime import datetime, timezone

import numpy as np
import pandas as pd

FECHA_BASE = datetime(2026, 5, 1, tzinfo=timezone.utc)   # la misma de scripts/preparar_ieee.py y producer/replayer.py
COLUMNAS = [
    "monto", "log_monto", "tipo", "region", "correo_dominio", "tiene_dispositivo", "hora", "dia_semana",
    "cli_tx_5min", "cli_tx_1h", "cli_tx_24h", "cli_tx_previas", "cli_seg_desde_anterior", "cli_monto_prom_30d",
    "monto_vs_prom_30d", "disp_nuevo_para_cliente", "disp_tx_previas",
]
CATEGORICAS = ["tipo", "region", "correo_dominio"]


def _conteo_ventana(t, g, segundos):
    """Filas del mismo grupo en (t - ventana, t], con `t` ordenado dentro de cada grupo (contando la propia)."""
    if len(t) == 0:
        return np.empty(0, dtype=np.int32)
    cambios = np.r_[0, np.flatnonzero(g[1:] != g[:-1]) + 1]
    out = np.empty(len(t), dtype=np.int32)
    for a, b in zip(cambios, np.r_[cambios[1:], len(g)]):
        tt = t[a:b]
        out[a:b] = np.arange(1, b - a + 1) - np.searchsorted(tt, tt - segundos, side="right")
    return out


def _vacio(s):
    """Texto vacío o NaN -> NaN (el CSV y el JSON del evento representan el nulo de formas distintas)."""
    return s.where(s.notna() & (s.astype(str).str.len() > 0))


def calcular(hist):
    """Devuelve las variables de cada fila de `hist` (mismo índice), en el orden de COLUMNAS."""
    d = hist[["id_transaccion", "t", "id_cliente", "id_dispositivo", "monto", "tipo", "region", "correo_dominio"]].copy()
    d["t"] = d["t"].astype("float64")
    d["monto"] = d["monto"].astype("float64")
    d["id_dispositivo"] = _vacio(d["id_dispositivo"])
    # Orden por cliente y tiempo; el id desempata (los TransactionID tienen el mismo largo: el orden de texto es el numérico).
    d = d.sort_values(["id_cliente", "t", "id_transaccion"], kind="mergesort")
    t, g = d["t"].to_numpy(), d["id_cliente"].to_numpy()
    for nombre, seg in (("cli_tx_5min", 300), ("cli_tx_1h", 3600), ("cli_tx_24h", 86400)):
        d[nombre] = _conteo_ventana(t, g, seg)
    por_cliente = d.groupby("id_cliente", sort=False)
    d["cli_tx_previas"] = por_cliente.cumcount()
    d["cli_seg_desde_anterior"] = por_cliente["t"].diff()
    ts = pd.to_datetime(d["t"], unit="s")
    prom = (d.assign(_ts=ts).set_index("_ts").groupby("id_cliente", sort=False)["monto"]
            .rolling("30D", closed="left").mean())
    d["cli_monto_prom_30d"] = prom.to_numpy()   # groupby+rolling conserva el orden por cliente y tiempo de `d`
    d["monto_vs_prom_30d"] = d["monto"] / d["cli_monto_prom_30d"]
    con_disp = d["id_dispositivo"].notna()
    d["disp_nuevo_para_cliente"] = (con_disp & ~d.duplicated(["id_cliente", "id_dispositivo"])
                                    & (d["cli_tx_previas"] > 0)).astype(int)
    d = d.sort_values(["t", "id_transaccion"], kind="mergesort")
    d["disp_tx_previas"] = d.groupby("id_dispositivo", sort=False).cumcount().where(d["id_dispositivo"].notna())
    x = pd.DataFrame({
        "monto": d["monto"], "log_monto": np.log1p(d["monto"]),
        "tipo": _vacio(d["tipo"]), "region": _vacio(d["region"]), "correo_dominio": _vacio(d["correo_dominio"]),
        "tiene_dispositivo": con_disp.astype(int),
        "hora": (d["t"] // 3600) % 24, "dia_semana": (d["t"] // 86400) % 7,
        **{c: d[c] for c in COLUMNAS[8:]},
    }, index=d.index)
    for c in CATEGORICAS:
        x[c] = x[c].astype("category")
    return x.loc[hist.index, COLUMNAS]


def segundos_desde_base(ts):
    """Serie de timestamps (UTC) -> segundos desde FECHA_BASE, como el `transaction_dt` de los eventos."""
    ts = pd.to_datetime(ts, utc=True)
    return (ts - pd.Timestamp(FECHA_BASE)).dt.total_seconds()

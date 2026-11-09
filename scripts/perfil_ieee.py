"""Perfil del dataset IEEE-CIS Fraud Detection (tarea B1).

Lee train_transaction.csv y train_identity.csv, calcula las cifras que usan la ficha de datos, el diagrama y la
calibración de las reglas, y las guarda en docs/datos/ieee-cis-perfil.json.

  python scripts/perfil_ieee.py --datos data/ieee-cis

Identificación del cliente: IEEE-CIS no trae un id de cliente. Se usa la aproximación conocida de la competencia:
id_cliente = card1 + addr1 + D1n, donde D1n = día de la transacción − D1 (D1 = días desde que se abrió la tarjeta),
así que D1n es el día de alta de la tarjeta y se mantiene constante para el mismo titular.
"""
import argparse
import json
import os

import numpy as np
import pandas as pd

COLS = ["TransactionID", "isFraud", "TransactionDT", "TransactionAmt", "ProductCD", "card1", "card4", "card6",
        "addr1", "P_emaildomain", "D1"]


def cargar(carpeta):
    tx = pd.read_csv(os.path.join(carpeta, "train_transaction.csv"), usecols=COLS)
    idn = pd.read_csv(os.path.join(carpeta, "train_identity.csv"), usecols=["TransactionID", "DeviceType", "DeviceInfo"])
    df = tx.merge(idn, on="TransactionID", how="left")
    df["dia"] = df["TransactionDT"] // 86400
    df["D1n"] = df["dia"] - df["D1"]
    df["id_cliente"] = (df["card1"].astype("Int64").astype(str) + "_" + df["addr1"].astype("Int64").astype(str)
                        + "_" + df["D1n"].astype("Int64").astype(str))
    df["hora_utc"] = (df["TransactionDT"] // 3600) % 24
    return df.sort_values("TransactionDT").reset_index(drop=True)


def max_en_ventana(df, segundos):
    """Máximo de transacciones de un mismo cliente dentro de cualquier ventana de `segundos` (vectorizado)."""
    s = df[["id_cliente", "TransactionDT", "TransactionAmt", "isFraud"]].sort_values(["id_cliente", "TransactionDT"])
    t = s["TransactionDT"].to_numpy()
    g = s["id_cliente"].to_numpy()
    # Para cada transacción: cuántas del mismo cliente hay en (t - ventana, t]. Dentro de cada cliente t está
    # ordenado, así que basta un searchsorted por grupo.
    cambios = np.r_[0, np.flatnonzero(g[1:] != g[:-1]) + 1]
    conteo = np.empty(len(t), dtype=np.int64)
    for a, b in zip(cambios, np.r_[cambios[1:], len(g)]):
        tt = t[a:b]
        conteo[a:b] = np.arange(1, b - a + 1) - np.searchsorted(tt, tt - segundos, side="right")
    return s.assign(n_ventana=conteo)


def perfil(df):
    p = {}
    n = len(df)
    p["filas"] = int(n)
    p["fraudes"] = int(df["isFraud"].sum())
    p["tasa_fraude"] = round(float(df["isFraud"].mean()), 5)
    p["dias_cubiertos"] = int(df["dia"].max() - df["dia"].min() + 1)
    p["tasa_por_segundo_real"] = round(n / (df["TransactionDT"].max() - df["TransactionDT"].min()), 4)
    p["con_dispositivo"] = {"filas": int(df["DeviceInfo"].notna().sum()),
                            "porcentaje": round(float(df["DeviceInfo"].notna().mean()) * 100, 2),
                            "tasa_fraude_con_identity": round(float(df.loc[df["DeviceType"].notna(), "isFraud"].mean()), 5)}
    p["nulos_pct"] = {c: round(float(df[c].isna().mean()) * 100, 2) for c in ("card1", "addr1", "D1", "P_emaildomain")}
    p["clientes"] = {"distintos": int(df["id_cliente"].nunique()),
                     "tx_por_cliente_p50": float(df.groupby("id_cliente").size().median()),
                     "tx_por_cliente_p99": float(df.groupby("id_cliente").size().quantile(0.99))}
    m = df["TransactionAmt"]
    p["monto_usd"] = {"p50": round(float(m.median()), 2), "p90": round(float(m.quantile(0.9)), 2),
                      "p99": round(float(m.quantile(0.99)), 2), "max": round(float(m.max()), 2),
                      "p50_fraude": round(float(m[df["isFraud"] == 1].median()), 2),
                      "p50_legitimo": round(float(m[df["isFraud"] == 0].median()), 2)}
    p["producto"] = (df.groupby("ProductCD")["isFraud"].agg(["size", "mean"]).round(4)
                     .rename(columns={"size": "filas", "mean": "tasa_fraude"}).to_dict(orient="index"))
    p["hora_utc_tasa_fraude"] = df.groupby("hora_utc")["isFraud"].mean().round(4).to_dict()
    top_correo = df.groupby("P_emaildomain")["isFraud"].agg(["size", "mean"])
    p["correo_mayor_tasa_min_500"] = (top_correo[top_correo["size"] >= 500].sort_values("mean", ascending=False)
                                      .head(8).round(4).rename(columns={"size": "filas", "mean": "tasa_fraude"})
                                      .to_dict(orient="index"))

    # Velocidad por cliente (calibración de R1): transacciones del mismo cliente en 5 minutos
    v = max_en_ventana(df, 300)
    p["r1_tx_en_5min"] = {"p50": float(v["n_ventana"].quantile(0.5)), "p99": float(v["n_ventana"].quantile(0.99)),
                          "p999": float(v["n_ventana"].quantile(0.999)), "max": int(v["n_ventana"].max()),
                          "tasa_fraude_si_>=3": round(float(v.loc[v["n_ventana"] >= 3, "isFraud"].mean()), 4),
                          "tasa_fraude_si_>=5": round(float(v.loc[v["n_ventana"] >= 5, "isFraud"].mean()), 4),
                          "tasa_fraude_si_1": round(float(v.loc[v["n_ventana"] == 1, "isFraud"].mean()), 4)}

    # Dispositivo nuevo para el cliente (base de R2), solo donde hay DeviceInfo
    d = df[df["DeviceInfo"].notna()].sort_values("TransactionDT")
    nuevo = ~d.duplicated(["id_cliente", "DeviceInfo"])
    con_historia = d["id_cliente"].duplicated()
    caso = nuevo & con_historia  # dispositivo nunca visto para un cliente que ya había comprado
    p["r2_dispositivo_nuevo"] = {"filas": int(caso.sum()),
                                 "tasa_fraude": round(float(d.loc[caso, "isFraud"].mean()), 4),
                                 "tasa_fraude_resto": round(float(d.loc[~caso, "isFraud"].mean()), 4)}
    return p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datos", default="data/ieee-cis")
    ap.add_argument("--salida", default="docs/datos/ieee-cis-perfil.json")
    a = ap.parse_args()
    df = cargar(a.datos)
    p = perfil(df)
    os.makedirs(os.path.dirname(a.salida), exist_ok=True)
    with open(a.salida, "w", encoding="utf-8") as f:
        json.dump(p, f, ensure_ascii=False, indent=2, default=str)
    print(json.dumps(p, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()

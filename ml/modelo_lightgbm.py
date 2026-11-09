"""Modelo de referencia LightGBM (tarea F2), evaluado igual que las reglas (docs/evaluacion/).

Entrena dos modelos con la MISMA partición temporal 80/20 de la evaluación de reglas:

  * "contrato": solo variables que FraudLens puede calcular en el streaming con el contrato del evento (monto, tipo,
    región, dominio de correo, dispositivo y velocidad/historia del cliente calculada as-of, sin mirar el futuro).
    Es el candidato a regla R4 en el streaming (tarea D5).
  * "vesta": todas las columnas originales de IEEE-CIS (C, D, M, V, identity...). No las tenemos en el evento, pero
    muestra hasta dónde llega un modelo con toda la información: la referencia del estado del arte en este dataset.

  docker build -f infra/docker/Dockerfile.ml -t fraudlens-ml .
  docker run --rm -v "$PWD:/opt/fraudlens" fraudlens-ml python ml/modelo_lightgbm.py

Salidas: docs/evaluacion/modelo.json (métricas e importancia de variables) y data/modelos/ (modelos entrenados,
fuera del repositorio).
"""
import argparse
import json
import os
import time

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

DATOS = "data/ieee-cis"
SALIDA_DOC = "docs/evaluacion/modelo.json"
SALIDA_MODELOS = "data/modelos"
FECHA_BASE = pd.Timestamp("2026-05-01")
# Presupuesto de alertas de las reglas calibradas en prueba (docs/evaluacion/resultados.json): 46,4 por cada 1.000.
PRESUPUESTO_REGLAS = 0.0464
RECALL_REGLAS = 0.1004
PRECISION_REGLAS = 0.0745


# ------------------------------------------------------------------ datos
COLS_CONTRATO = ["TransactionID", "isFraud", "TransactionDT", "TransactionAmt", "ProductCD", "card1", "addr1",
                 "P_emaildomain", "D1", "DeviceInfo"]


def _leer_compacto(ruta, columnas=None):
    """Lee el CSV por partes y con las columnas numéricas en float32: el dataset completo (~430 columnas) no cabe
    en memoria si se lee de una sola vez en float64."""
    muestra = pd.read_csv(ruta, nrows=2000)
    cols = [c for c in muestra.columns if columnas is None or c in columnas]
    enteras = {"TransactionID": "int64", "isFraud": "int8", "TransactionDT": "int64"}
    tipos = {c: enteras.get(c, "float32") for c in cols if muestra[c].dtype != object}
    partes = pd.read_csv(ruta, usecols=cols, dtype=tipos, chunksize=100_000)
    return pd.concat(partes, ignore_index=True)


def cargar(carpeta, columnas=None):
    tx = _leer_compacto(os.path.join(carpeta, "train_transaction.csv"), columnas)
    idn = _leer_compacto(os.path.join(carpeta, "train_identity.csv"), columnas)
    tx = tx.merge(idn, on="TransactionID", how="left")
    del idn
    df = tx.sort_values(["TransactionDT", "TransactionID"], ignore_index=True)
    del tx

    def texto(s):
        return s.astype("Int64").astype(str).replace("<NA>", "NA")

    df["id_cliente"] = "U" + texto(df["card1"]) + "_" + texto(df["addr1"]) + "_" + texto(df["TransactionDT"] // 86400 - df["D1"])
    corte = json.load(open(os.path.join(carpeta, "preparado", "particion.json"), encoding="utf-8"))["corte_transaction_dt"]
    df["particion"] = np.where(df["TransactionDT"] > corte, "prueba", "entrenamiento")
    df["ts"] = FECHA_BASE + pd.to_timedelta(df["TransactionDT"], unit="s")
    return df


def _conteo_ventana(t, g, segundos):
    """Transacciones del mismo grupo en (t - ventana, t], con t ordenado dentro de cada grupo."""
    cambios = np.r_[0, np.flatnonzero(g[1:] != g[:-1]) + 1]
    out = np.empty(len(t), dtype=np.int32)
    for a, b in zip(cambios, np.r_[cambios[1:], len(g)]):
        tt = t[a:b]
        out[a:b] = np.arange(1, b - a + 1) - np.searchsorted(tt, tt - segundos, side="right")
    return out


def variables_contrato(df):
    """Variables que el streaming puede calcular con el evento y la historia previa del cliente (as-of)."""
    d = df.sort_values(["id_cliente", "TransactionDT"]).copy()
    t, g = d["TransactionDT"].to_numpy(), d["id_cliente"].to_numpy()
    for nombre, seg in (("cli_tx_5min", 300), ("cli_tx_1h", 3600), ("cli_tx_24h", 86400)):
        d[nombre] = _conteo_ventana(t, g, seg)
    d["cli_tx_previas"] = d.groupby("id_cliente").cumcount()
    d["cli_seg_desde_anterior"] = d.groupby("id_cliente")["TransactionDT"].diff()
    prom = (d.set_index("ts").groupby("id_cliente")["TransactionAmt"].rolling("30D", closed="left").mean()
            .reset_index(level=0, drop=True))
    d["cli_monto_prom_30d"] = prom.to_numpy()
    d["monto_vs_prom_30d"] = d["TransactionAmt"] / d["cli_monto_prom_30d"]
    con_disp = d["DeviceInfo"].notna()
    d["disp_nuevo_para_cliente"] = (con_disp & ~d.duplicated(["id_cliente", "DeviceInfo"]) & (d["cli_tx_previas"] > 0)).astype(int)
    d = d.sort_values(["TransactionDT", "TransactionID"])
    d["disp_tx_previas"] = d.groupby("DeviceInfo").cumcount().where(d["DeviceInfo"].notna())
    x = pd.DataFrame({
        "monto": d["TransactionAmt"], "log_monto": np.log1p(d["TransactionAmt"]),
        "tipo": d["ProductCD"].astype("category"), "region": d["addr1"].astype("category"),
        "correo_dominio": d["P_emaildomain"].astype("category"), "tiene_dispositivo": con_disp.astype(int),
        "hora": (d["TransactionDT"] // 3600) % 24, "dia_semana": (d["TransactionDT"] // 86400) % 7,
        "cli_tx_5min": d["cli_tx_5min"], "cli_tx_1h": d["cli_tx_1h"], "cli_tx_24h": d["cli_tx_24h"],
        "cli_tx_previas": d["cli_tx_previas"], "cli_seg_desde_anterior": d["cli_seg_desde_anterior"],
        "cli_monto_prom_30d": d["cli_monto_prom_30d"], "monto_vs_prom_30d": d["monto_vs_prom_30d"],
        "disp_nuevo_para_cliente": d["disp_nuevo_para_cliente"], "disp_tx_previas": d["disp_tx_previas"],
    }, index=d.index)
    return x.loc[df.index]


def variables_vesta(df):
    """Todas las columnas originales de IEEE-CIS (sin id, etiqueta ni tiempo absoluto)."""
    fuera = {"TransactionID", "isFraud", "TransactionDT", "id_cliente", "particion", "ts"}
    x = df[[c for c in df.columns if c not in fuera]]
    objetos = [c for c in x.columns if x[c].dtype == object]
    return x.assign(**{c: x[c].astype("category") for c in objetos})


# ------------------------------------------------------------------ entrenamiento y evaluación
def entrenar(x, y, particion):
    tr = np.flatnonzero(particion == "entrenamiento")
    corte_val = int(len(tr) * 0.9)          # el último 10 % del entrenamiento (en el tiempo) para parar a tiempo
    i_fit, i_val = tr[:corte_val], tr[corte_val:]
    modelo = lgb.LGBMClassifier(
        n_estimators=2000, learning_rate=0.05, num_leaves=63, min_child_samples=100, subsample=0.8, subsample_freq=1,
        colsample_bytree=0.8, reg_lambda=1.0, random_state=42, verbose=-1)
    modelo.fit(x.iloc[i_fit], y[i_fit], eval_set=[(x.iloc[i_val], y[i_val])], eval_metric="auc",
               callbacks=[lgb.early_stopping(100, verbose=False)])
    return modelo


def punto_operacion(y, score, monto, alertadas):
    tp = int(((alertadas == 1) & (y == 1)).sum())
    fp = int(((alertadas == 1) & (y == 0)).sum())
    fn = int(((alertadas == 0) & (y == 1)).sum())
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    return {"alertadas": tp + fp, "tp": tp, "fp": fp, "fn": fn, "precision": round(prec, 4), "recall": round(rec, 4),
            "alertas_por_mil": round(1000 * (tp + fp) / len(y), 2),
            "monto_fraude_detectado_pct": round(float(100 * monto[(alertadas == 1) & (y == 1)].sum() / monto[y == 1].sum()), 2)}


def evaluar(modelo, x, y, monto, particion):
    te = np.flatnonzero(particion == "prueba")
    s = modelo.predict_proba(x.iloc[te])[:, 1]
    yt, mt = y[te], monto[te]
    res = {"auc_roc": round(float(roc_auc_score(yt, s)), 4), "auc_pr": round(float(average_precision_score(yt, s)), 4),
           "arboles": int(modelo.best_iteration_ or modelo.n_estimators)}
    # 1) Mismo presupuesto de alertas que las reglas calibradas (46,4 por cada 1.000 pagos)
    k = int(round(PRESUPUESTO_REGLAS * len(te)))
    alert = np.zeros(len(te), dtype=int)
    alert[np.argsort(-s)[:k]] = 1
    res["mismo_presupuesto_que_reglas"] = punto_operacion(yt, s, mt, alert)
    # 2) Misma cobertura que las reglas (10 %): ¿con qué precisión?
    orden = np.argsort(-s)
    acumulado = np.cumsum(yt[orden]) / yt.sum()
    k2 = int(np.searchsorted(acumulado, RECALL_REGLAS) + 1)
    alert2 = np.zeros(len(te), dtype=int)
    alert2[orden[:k2]] = 1
    res["misma_cobertura_que_reglas"] = punto_operacion(yt, s, mt, alert2)
    return res


def importancia(modelo, x, n=10):
    imp = pd.Series(modelo.booster_.feature_importance("gain"), index=x.columns).sort_values(ascending=False)
    return {k: round(float(v / imp.sum()) * 100, 1) for k, v in imp.head(n).items()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datos", default=DATOS)
    a = ap.parse_args()
    t0 = time.time()
    salida = {"reglas_calibradas_referencia": {"precision": PRECISION_REGLAS, "recall": RECALL_REGLAS,
                                               "alertas_por_mil": PRESUPUESTO_REGLAS * 1000},
              "modelos": {}}
    os.makedirs(SALIDA_MODELOS, exist_ok=True)
    # Cada modelo carga solo lo que necesita, para no tener las ~430 columnas en memoria dos veces.
    for nombre, construir, columnas in (("contrato", variables_contrato, COLS_CONTRATO), ("vesta", variables_vesta, None)):
        df = cargar(a.datos, columnas)
        y, monto, particion = df["isFraud"].to_numpy(), df["TransactionAmt"].to_numpy(), df["particion"].to_numpy()
        salida["particion_prueba"] = {"transacciones": int((particion == "prueba").sum()),
                                      "tasa_base": round(float(y[particion == "prueba"].mean()), 5)}
        x = construir(df)
        del df
        modelo = entrenar(x, y, particion)
        r = evaluar(modelo, x, y, monto, particion)
        r["variables"] = int(x.shape[1])
        r["importancia_top10_pct"] = importancia(modelo, x)
        salida["modelos"][nombre] = r
        modelo.booster_.save_model(os.path.join(SALIDA_MODELOS, "lightgbm_%s.txt" % nombre))
        print(nombre, json.dumps({k: r[k] for k in ("auc_roc", "auc_pr", "arboles", "variables")}),
              "| presupuesto:", json.dumps(r["mismo_presupuesto_que_reglas"]), flush=True)
        del x, modelo   # liberar memoria antes del siguiente modelo
    salida["duracion_s"] = round(time.time() - t0, 1)
    os.makedirs(os.path.dirname(SALIDA_DOC), exist_ok=True)
    with open(SALIDA_DOC, "w", encoding="utf-8") as f:
        json.dump(salida, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()

"""Comparación final (tarea F3): reglas, modelo y sus combinaciones, en la misma partición de prueba de IEEE-CIS.

Junta dos archivos que quedan en data/ (fuera del repositorio):
  * data/evaluacion/marcas_prueba_ieee.csv: qué reglas marcaron cada transacción (python -m batch.evaluacion exportar)
  * data/modelos/puntajes_prueba.csv: el puntaje de cada modelo LightGBM (python ml/modelo_lightgbm.py)

Escenarios (todos sobre las mismas 118.108 transacciones de prueba):
  1. Reglas calibradas solas (lo que hace hoy el streaming).
  2. Modelo "contrato" solo, con el mismo presupuesto de alertas que las reglas.
  3. Reglas filtradas por el modelo: de las alertas de reglas se conserva solo la fracción con mayor puntaje
     (75 %, 50 % y 25 %). Las fracciones son fijas: no se eligen mirando las etiquetas de prueba.
  4. Combinaciones con el mismo presupuesto: se conservan unas alertas de reglas y el modelo llena el cupo que queda.
     a) R2 y R3 (las reglas precisas) + modelo, sin R1 (la ruidosa).
     b) La mitad de las alertas de reglas con mayor puntaje + modelo.
  5. Modelo "vesta" (referencia del estado del arte, con variables que el evento no trae).

  docker run --rm -v "$PWD:/opt/fraudlens" fraudlens-ml python ml/comparacion_final.py

Salidas: docs/evaluacion/comparacion_final.json y comparacion_final.md.
"""
import argparse
import json
import os

import numpy as np
import pandas as pd

MARCAS = "data/evaluacion/marcas_prueba_ieee.csv"
PUNTAJES = "data/modelos/puntajes_prueba.csv"
SALIDA = "docs/evaluacion"


# ------------------------------------------------------------------ selecciones de alertas (vectores 0/1)
def top_k(puntaje, k):
    """Las k transacciones de mayor puntaje (empates por orden estable)."""
    alertas = np.zeros(len(puntaje), dtype=int)
    alertas[np.argsort(-puntaje, kind="stable")[:max(int(k), 0)]] = 1
    return alertas


def filtrar_por_puntaje(reglas, puntaje, fraccion):
    """De las transacciones marcadas por reglas, conserva la `fraccion` con mayor puntaje del modelo."""
    idx = np.flatnonzero(reglas == 1)
    k = int(round(fraccion * len(idx)))
    alertas = np.zeros(len(reglas), dtype=int)
    alertas[idx[np.argsort(-puntaje[idx], kind="stable")[:k]]] = 1
    return alertas


def hibrido(reglas, puntaje, presupuesto):
    """Conserva las alertas de `reglas` y llena el cupo hasta `presupuesto` con las de mayor puntaje que no marcaron."""
    alertas = reglas.astype(int).copy()
    libres = np.flatnonzero(alertas == 0)
    cupo = max(int(presupuesto) - int(alertas.sum()), 0)
    alertas[libres[np.argsort(-puntaje[libres], kind="stable")[:cupo]]] = 1
    return alertas


# ------------------------------------------------------------------ métricas
def metricas(y, monto, alertas):
    tp = int(((alertas == 1) & (y == 1)).sum())
    fp = int(((alertas == 1) & (y == 0)).sum())
    fn = int(((alertas == 0) & (y == 1)).sum())
    n, legitimas = len(y), int((y == 0).sum())
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    return {
        "alertadas": tp + fp, "tp": tp, "fp": fp, "fn": fn,
        "precision": round(prec, 4), "recall": round(rec, 4),
        "f1": round(2 * prec * rec / (prec + rec), 4) if prec + rec else 0.0,
        "alertas_por_mil": round(1000 * (tp + fp) / n, 2),
        "falsos_positivos_por_mil_legitimas": round(1000 * fp / legitimas, 2) if legitimas else 0.0,
        "monto_fraude_detectado_pct": round(float(100 * monto[(alertas == 1) & (y == 1)].sum() / monto[y == 1].sum()), 2),
    }


def escenarios(df):
    y, monto = df["is_fraud"].to_numpy(), df["monto"].to_numpy()
    reglas = ((df["r1"] + df["r2"] + df["r3"]) > 0).astype(int).to_numpy()
    s = df["puntaje_contrato"].to_numpy()
    presupuesto = int(reglas.sum())
    filas = [("Reglas calibradas (R1, R2 o R3)", "reglas", reglas),
             ("Modelo \"contrato\" (mismo presupuesto)", "modelo", top_k(s, presupuesto))]
    for f in (0.75, 0.5, 0.25):
        filas.append(("Reglas filtradas por el modelo (se conserva el %d %%)" % (100 * f), "filtro",
                      filtrar_por_puntaje(reglas, s, f)))
    r23 = ((df["r2"] + df["r3"]) > 0).astype(int).to_numpy()
    filas.append(("R2 y R3 + modelo (sin R1), mismo presupuesto", "hibrido", hibrido(r23, s, presupuesto)))
    filas.append(("Mitad de las reglas con mayor puntaje + modelo, mismo presupuesto", "hibrido",
                  hibrido(filtrar_por_puntaje(reglas, s, 0.5), s, presupuesto)))
    if "puntaje_vesta" in df:
        filas.append(("Modelo \"vesta\" (referencia, mismo presupuesto)", "referencia",
                      top_k(df["puntaje_vesta"].to_numpy(), presupuesto)))
    salida = []
    for nombre, tipo, alertas in filas:
        salida.append({"escenario": nombre, "tipo": tipo, **metricas(y, monto, alertas)})
    # Qué tan bien ordena el modelo las alertas de cada regla: precisión de la mitad con mayor puntaje
    por_regla = {}
    for r in ("r1", "r2", "r3"):
        marc = df[r].to_numpy()
        por_regla[r.upper()] = {"todas": metricas(y, monto, marc),
                                "mitad_con_mayor_puntaje": metricas(y, monto, filtrar_por_puntaje(marc, s, 0.5))}
    return {"transacciones": len(df), "tasa_base": round(float(y.mean()), 5), "presupuesto_alertas": presupuesto,
            "escenarios": salida, "por_regla": por_regla}


def a_markdown(res):
    lineas = ["# Comparación final: reglas, modelo y combinaciones (F3)", "",
              "Partición de **prueba** de IEEE-CIS: %d transacciones, tasa base de fraude **%.2f %%**. "
              "Generado con `python ml/comparacion_final.py`." % (res["transacciones"], 100 * res["tasa_base"]), "",
              "| Escenario | Alertas/1.000 | Precisión | Cobertura | F1 | Falsos positivos | FP por 1.000 legítimas | Monto de fraude detectado |",
              "|---|---|---|---|---|---|---|---|"]
    for e in res["escenarios"]:
        lineas.append("| %s | %.1f | %.1f %% | %.1f %% | %.3f | %d | %.1f | %.1f %% |" % (
            e["escenario"], e["alertas_por_mil"], 100 * e["precision"], 100 * e["recall"], e["f1"], e["fp"],
            e["falsos_positivos_por_mil_legitimas"], e["monto_fraude_detectado_pct"]))
    lineas += ["", "Precisión de cada regla, sola y quedándose con la mitad de sus alertas de mayor puntaje:", "",
               "| Regla | Alertas | Precisión | Mitad con mayor puntaje: precisión | Cobertura que conserva |", "|---|---|---|---|---|"]
    for r, d in res["por_regla"].items():
        t, m = d["todas"], d["mitad_con_mayor_puntaje"]
        conserva = 100 * m["tp"] / t["tp"] if t["tp"] else 0.0
        lineas.append("| %s | %d | %.1f %% | %.1f %% | %.0f %% de sus aciertos |" % (
            r, t["alertadas"], 100 * t["precision"], 100 * m["precision"], conserva))
    return "\n".join(lineas) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--marcas", default=MARCAS)
    ap.add_argument("--puntajes", default=PUNTAJES)
    a = ap.parse_args()
    df = pd.read_csv(a.marcas).merge(pd.read_csv(a.puntajes), on="id_transaccion", how="inner", validate="1:1")
    marcas_n = len(pd.read_csv(a.marcas, usecols=["id_transaccion"]))
    assert len(df) == marcas_n, "las marcas y los puntajes no cubren las mismas transacciones (%d vs %d)" % (len(df), marcas_n)
    res = escenarios(df)
    os.makedirs(SALIDA, exist_ok=True)
    with open(os.path.join(SALIDA, "comparacion_final.json"), "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=2)
    with open(os.path.join(SALIDA, "comparacion_final.md"), "w", encoding="utf-8") as f:
        f.write(a_markdown(res))
    print(a_markdown(res))


if __name__ == "__main__":
    main()

"""Calibración (D2) y evaluación (F1) de las reglas R1-R3 contra las etiquetas reales de IEEE-CIS.

Usa EXACTAMENTE las mismas funciones de reglas del sistema (common/rules.py), no una copia, sobre los eventos
preparados por scripts/preparar_ieee.py:

  python -m batch.evaluacion calibrar --datos data/ieee-cis/preparado        # D2: solo partición de entrenamiento
  python -m batch.evaluacion evaluar  --datos data/ieee-cis/preparado        # F1: partición de prueba, v1 vs ieee

Reglas de evaluación (iguales a Gold, `marcar_sospechosas`):
  * una transacción queda ALERTADA si tiene alerta propia (R2, R3) o si cae dentro de una ventana de R1 que alertó;
  * se compara contra isFraud a nivel de transacción: matriz de confusión, precisión, cobertura (recall), F1,
    tasa de falsos positivos, alertas por cada 1.000 transacciones y % del monto de fraude detectado.
Calibrar mira SOLO el 80 % inicial (entrenamiento); evaluar mira SOLO el 20 % final (prueba), con la historia
anterior disponible para R1 y R2 (as-of), como pasaría en producción.

Resultados en docs/evaluacion/ (calibracion.json, resultados.json y resultados.md).
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from common import rules
from common.spark import get_spark

FECHA_BASE = datetime(2026, 5, 1, tzinfo=timezone.utc)   # la misma de scripts/preparar_ieee.py
SALIDA = "docs/evaluacion"
MAX_ALERTAS_POR_MIL = 50   # restricción operativa al calibrar: el analista no puede revisar más del 5 % de los pagos

ESQUEMA_EVENTOS = StructType([StructField(c, t) for c, t in [
    ("transaction_dt", DoubleType()), ("particion", StringType()), ("id_transaccion", StringType()),
    ("id_cliente", StringType()), ("id_comercio", StringType()), ("id_dispositivo", StringType()),
    ("monto", DoubleType()), ("tipo", StringType()), ("region", StringType()), ("correo_dominio", StringType())]])


# ------------------------------------------------------------------ datos
def cargar(spark, datos):
    ev = spark.read.option("header", True).schema(ESQUEMA_EVENTOS).csv(datos.rstrip("/") + "/eventos.csv.gz")
    et = (spark.read.option("header", True).csv(datos.rstrip("/") + "/etiquetas.csv.gz")
          .select("id_transaccion", F.col("isFraud").cast("int").alias("is_fraud")))
    base = int(FECHA_BASE.timestamp())
    tx = (ev.withColumn("ts_evento", F.timestamp_seconds(F.lit(base) + F.col("transaction_dt")))
          .withColumn("kafka_partition", F.lit(None).cast("int"))
          .withColumn("kafka_offset", F.lit(None).cast("bigint"))
          .join(et, "id_transaccion"))
    lista = (spark.read.option("header", True).csv(datos.rstrip("/") + "/riesgo_v100.csv")
             .withColumn("version", F.lit(100)))
    return tx.cache(), lista.cache()


# ------------------------------------------------------------------ transacciones alertadas por regla
def _registrar(nombre, **params):
    base = dict(rules.RULES["v1"])
    base.update(params)
    rules.RULES[nombre] = base
    return nombre


def marcadas_r1(tx, agregado, version):
    """Transacciones que caen en una ventana de R1 que alertó (igual que Gold)."""
    al = rules.r1_alertas(agregado, version).select(
        F.col("id_cliente").alias("_c"), F.col("ventana_inicio").alias("_v0"), F.col("ventana_fin").alias("_v1"))
    return (tx.select("id_transaccion", "id_cliente", "ts_evento")
            .join(F.broadcast(al), (F.col("id_cliente") == F.col("_c")) & (F.col("ts_evento") >= F.col("_v0"))
                  & (F.col("ts_evento") < F.col("_v1")))
            .select("id_transaccion").distinct())


def marcadas_r2(tx, version):
    return rules.r2_alertas_historico(tx, version).select("id_transaccion").distinct()


def marcadas_r3(tx, lista, version):
    return rules.r3_alertas(tx, rules.lista_vigente(lista), version).select("id_transaccion").distinct()


# ------------------------------------------------------------------ métricas
def metricas(tx_eval, marcadas):
    """Métricas a nivel de transacción sobre `tx_eval` (id_transaccion, is_fraud, monto)."""
    m = (tx_eval.join(marcadas.withColumn("_alerta", F.lit(1)), "id_transaccion", "left")
         .withColumn("_alerta", F.coalesce("_alerta", F.lit(0))))
    r = m.agg(
        F.count("*").alias("n"),
        F.sum(F.col("_alerta") * F.col("is_fraud")).alias("tp"),
        F.sum(F.col("_alerta") * (1 - F.col("is_fraud"))).alias("fp"),
        F.sum((1 - F.col("_alerta")) * F.col("is_fraud")).alias("fn"),
        F.sum(F.when(F.col("is_fraud") == 1, F.col("monto"))).alias("monto_fraude"),
        F.sum(F.when((F.col("is_fraud") == 1) & (F.col("_alerta") == 1), F.col("monto"))).alias("monto_detectado"),
    ).collect()[0]
    n, tp, fp, fn = int(r["n"]), int(r["tp"] or 0), int(r["fp"] or 0), int(r["fn"] or 0)
    tn = n - tp - fp - fn
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    return {
        "transacciones": n, "alertadas": tp + fp, "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": round(prec, 4), "recall": round(rec, 4),
        "f1": round(2 * prec * rec / (prec + rec), 4) if prec + rec else 0.0,
        "tasa_falsos_positivos": round(fp / (fp + tn), 4) if fp + tn else 0.0,
        "alertas_por_mil": round(1000 * (tp + fp) / n, 2) if n else 0.0,
        "monto_fraude_detectado_pct": round(100 * float(r["monto_detectado"] or 0) / float(r["monto_fraude"] or 1), 2),
    }


def evaluar_version(tx, lista, agregado, version, particion):
    """Métricas de R1, R2, R3 y su unión para una versión de reglas, sobre una partición."""
    tx_eval = tx.where(F.col("particion") == particion).select("id_transaccion", "is_fraud", "monto").cache()
    r1, r2, r3 = marcadas_r1(tx, agregado, version), marcadas_r2(tx, version), marcadas_r3(tx, lista, version)
    r1, r2, r3 = r1.cache(), r2.cache(), r3.cache()
    res = {"R1": metricas(tx_eval, r1), "R2": metricas(tx_eval, r2), "R3": metricas(tx_eval, r3),
           "Cualquiera (R1, R2 o R3)": metricas(tx_eval, r1.union(r2).union(r3).distinct())}
    base = tx_eval.agg(F.avg("is_fraud")).collect()[0][0]
    res["_tasa_base"] = round(float(base), 5)
    return res


# ------------------------------------------------------------------ D2: calibración
def calibrar(spark, datos):
    tx, lista = cargar(spark, datos)
    agregado = rules.r1_agregar(tx.select("ts_evento", "id_cliente", "monto")).cache()
    tx_tr = tx.where(F.col("particion") == "entrenamiento").select("id_transaccion", "is_fraud", "monto").cache()
    salida = {"particion": "entrenamiento", "restriccion": "alertas_por_mil <= %d" % MAX_ALERTAS_POR_MIL, "r1": [], "r2": []}

    for max_tx in (1, 2, 3, 4, 5):
        for max_monto in (1000, 2000, 5000, 10**12):
            v = _registrar("_cal_r1", r1_max_tx=max_tx, r1_max_monto=max_monto)
            m = metricas(tx_tr, marcadas_r1(tx, agregado, v))
            salida["r1"].append(dict(r1_max_tx=max_tx, r1_max_monto_usd=None if max_monto == 10**12 else max_monto, **m))
            print("R1 max_tx=%d max_monto=%s -> precision %.3f recall %.3f f1 %.3f alertas/1000 %.1f" % (
                max_tx, max_monto, m["precision"], m["recall"], m["f1"], m["alertas_por_mil"]), flush=True)
    for factor in (1.5, 2.0, 3.0, 5.0, 8.0):
        v = _registrar("_cal_r2", r2_factor=factor)
        m = metricas(tx_tr, marcadas_r2(tx, v))
        salida["r2"].append(dict(r2_factor=factor, **m))
        print("R2 factor=%.1f -> precision %.3f recall %.3f f1 %.3f alertas/1000 %.1f" % (
            factor, m["precision"], m["recall"], m["f1"], m["alertas_por_mil"]), flush=True)

    def mejor(filas):
        ok = [f for f in filas if f["alertas_por_mil"] <= MAX_ALERTAS_POR_MIL] or filas
        return max(ok, key=lambda f: (f["f1"], f["precision"]))

    b1, b2 = mejor(salida["r1"]), mejor(salida["r2"])
    salida["elegido"] = {"r1_max_tx": b1["r1_max_tx"], "r1_max_monto_usd": b1["r1_max_monto_usd"],
                         "r2_factor": b2["r2_factor"], "criterio": "máximo F1 en entrenamiento con la restricción"}
    _guardar("calibracion.json", salida)
    print("ELEGIDO:", json.dumps(salida["elegido"], ensure_ascii=False))
    return salida


# ------------------------------------------------------------------ F1: evaluación
def evaluar(spark, datos, versiones):
    tx, lista = cargar(spark, datos)
    agregado = rules.r1_agregar(tx.select("ts_evento", "id_cliente", "monto")).cache()
    salida = {"particion": "prueba", "versiones": {}}
    for v in versiones:
        salida["versiones"][v] = {"parametros": rules.config_version(v),
                                  "metricas": evaluar_version(tx, lista, agregado, v, "prueba")}
        print(v, json.dumps(salida["versiones"][v]["metricas"]["Cualquiera (R1, R2 o R3)"]), flush=True)
    _guardar("resultados.json", salida)
    _guardar_markdown(salida)
    return salida


def _guardar(nombre, obj):
    os.makedirs(SALIDA, exist_ok=True)
    with open(os.path.join(SALIDA, nombre), "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _guardar_markdown(salida):
    filas = ["| Versión | Regla | Alertadas | Precisión | Cobertura (recall) | F1 | Falsos positivos | Alertas/1.000 | Monto de fraude detectado |",
             "|---|---|---|---|---|---|---|---|---|"]
    for v, d in salida["versiones"].items():
        for regla, m in d["metricas"].items():
            if regla.startswith("_"):
                continue
            filas.append("| %s | %s | %d | %.1f %% | %.1f %% | %.3f | %d (%.2f %%) | %.1f | %.1f %% |" % (
                v, regla, m["alertadas"], 100 * m["precision"], 100 * m["recall"], m["f1"], m["fp"],
                100 * m["tasa_falsos_positivos"], m["alertas_por_mil"], m["monto_fraude_detectado_pct"]))
    base = next(iter(salida["versiones"].values()))["metricas"]["_tasa_base"]
    texto = ["# Evaluación de las reglas contra etiquetas reales (IEEE-CIS)", "",
             "Partición de **prueba** (20 %% final en el tiempo, %d transacciones), tasa base de fraude **%.2f %%**. "
             % (next(iter(salida["versiones"].values()))["metricas"]["R1"]["transacciones"], 100 * base)
             + "Generado con `python -m batch.evaluacion evaluar`.", ""] + filas
    with open(os.path.join(SALIDA, "resultados.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(texto) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paso", choices=["calibrar", "evaluar"])
    ap.add_argument("--datos", default="data/ieee-cis/preparado", help="carpeta (o URI del lake) con los datos preparados")
    ap.add_argument("--versiones", nargs="+", default=["v1", "ieee"])
    a = ap.parse_args()
    spark = get_spark("fraudlens-evaluacion", shuffle_partitions=16)
    try:
        calibrar(spark, a.datos) if a.paso == "calibrar" else evaluar(spark, a.datos, a.versiones)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

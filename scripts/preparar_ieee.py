"""Prepara IEEE-CIS para FraudLens (tarea B2): eventos, etiquetas, partición temporal, catálogos y lista de riesgo.

  python scripts/preparar_ieee.py --datos data/ieee-cis
  python scripts/preparar_ieee.py --datos data/ieee-cis --destino s3a://fraudlens-lake-216408407230   # además sube

Genera en <datos>/preparado/:
  eventos.csv.gz           una fila por transacción, en orden de TransactionDT, con los campos del contrato
                           (SIN la etiqueta isFraud: nunca viaja por el stream). La lee producer/replayer.py.
  etiquetas.csv.gz         id_transaccion, isFraud, particion: la "verdad" para evaluar (tarea F1)
  particion.json           corte temporal 80/20: el 80 % inicial calibra y entrena, el 20 % final solo evalúa
  catalogos/clientes.csv   clientes aproximados (card1 + addr1 + D1n), en el formato de producer/catalogo.py
  catalogos/comercios.csv  los 5 tipos de producto como "líneas de negocio"
  riesgo_v100.csv          lista de riesgo de R3: dispositivos con fraude alto, aprendida SOLO con el 80 % inicial
  resumen.json             cifras de la preparación

Con --destino sube todo al lakehouse (S3 o MinIO, según S3_ENDPOINT): raw/ieee-cis/preparado/... y la lista a
landing/listas_riesgo/riesgo_v100.csv, donde la toma el DAG catalogos_diario.
"""
import argparse
import gzip
import json
import os
import sys

import pandas as pd

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

VERSION_LISTA = 100            # versión de la lista de riesgo de IEEE-CIS (las sintéticas usan 1, 2, ...)
FRACCION_ENTRENAMIENTO = 0.8
LISTA_MIN_TX = 30              # un dispositivo entra a la lista si tiene al menos 30 transacciones de entrenamiento
LISTA_MIN_TASA = 0.20          # y una tasa de fraude de 20 % o más (unas 6 veces la tasa base de 3,5 %)
FECHA_BASE = "2026-05-01"      # ts_evento = FECHA_BASE + TransactionDT; los 182 días terminan a finales de octubre de 2026


def cargar(carpeta):
    tx = pd.read_csv(os.path.join(carpeta, "train_transaction.csv"),
                     usecols=["TransactionID", "isFraud", "TransactionDT", "TransactionAmt", "ProductCD",
                              "card1", "card6", "addr1", "P_emaildomain", "D1"])
    idn = pd.read_csv(os.path.join(carpeta, "train_identity.csv"), usecols=["TransactionID", "DeviceInfo"])
    df = tx.merge(idn, on="TransactionID", how="left").sort_values(["TransactionDT", "TransactionID"]).reset_index(drop=True)

    def texto(serie):
        return serie.astype("Int64").astype(str).replace("<NA>", "NA")

    df["D1n"] = df["TransactionDT"] // 86400 - df["D1"]
    df["id_cliente"] = "U" + texto(df["card1"]) + "_" + texto(df["addr1"]) + "_" + texto(df["D1n"])
    df["id_transaccion"] = "T" + df["TransactionID"].astype(str)
    df["region"] = ("R" + texto(df["addr1"])).replace("RNA", None)
    return df


def particion(df):
    corte = float(df["TransactionDT"].quantile(FRACCION_ENTRENAMIENTO))
    df["particion"] = (df["TransactionDT"] > corte).map({False: "entrenamiento", True: "prueba"})
    base = pd.Timestamp(FECHA_BASE, tz="UTC")
    return {
        "fraccion_entrenamiento": FRACCION_ENTRENAMIENTO,
        "corte_transaction_dt": corte,
        "corte_ts_evento": (base + pd.to_timedelta(round(corte), unit="s")).isoformat(),
        "fecha_base": FECHA_BASE,
        "filas": df["particion"].value_counts().to_dict(),
        "tasa_fraude": df.groupby("particion")["isFraud"].mean().round(5).to_dict(),
    }


def lista_riesgo(df):
    """Dispositivos con fraude alto, calculados SOLO con la partición de entrenamiento (no se mira el futuro)."""
    tr = df[(df["particion"] == "entrenamiento") & df["DeviceInfo"].notna()]
    g = tr.groupby("DeviceInfo")["isFraud"].agg(["size", "mean"])
    sel = g[(g["size"] >= LISTA_MIN_TX) & (g["mean"] >= LISTA_MIN_TASA)].sort_values("mean", ascending=False)
    filas = [("DISPOSITIVO", disp, "fraude %.0f %% en %d transacciones (entrenamiento)" % (100 * r["mean"], r["size"]))
             for disp, r in sel.iterrows()]
    return pd.DataFrame(filas, columns=["tipo", "id_entidad", "motivo"])


def catalogos(df):
    base = pd.Timestamp(FECHA_BASE)
    c = df.drop_duplicates("id_cliente")[["id_cliente", "card1", "card6", "region", "D1n"]]
    clientes = pd.DataFrame({
        "id_cliente": c["id_cliente"],
        "nombre": "Cliente " + c["id_cliente"],
        "documento": "IEEE" + c["card1"].astype("Int64").astype(str),   # no es un dato personal real
        "telefono": None,
        "correo": None,
        "ciudad": c["region"].fillna("Desconocida"),
        "segmento": c["card6"].fillna("desconocido"),
        "fecha_alta": (base + pd.to_timedelta(c["D1n"].fillna(0).clip(lower=0), unit="D")).dt.strftime("%Y-%m-%d"),
    })
    comercios = pd.DataFrame({
        "id_comercio": ["W", "C", "R", "H", "S"],
        "nombre": ["Producto W", "Producto C", "Producto R", "Producto H", "Producto S"],
        "ciudad": "N/D",
        "categoria": "producto",
    })
    return clientes, comercios


def eventos(df):
    return pd.DataFrame({
        "transaction_dt": df["TransactionDT"],
        "particion": df["particion"],
        "id_transaccion": df["id_transaccion"],
        "id_cliente": df["id_cliente"],
        "id_comercio": df["ProductCD"],
        "id_dispositivo": df["DeviceInfo"],
        "monto": df["TransactionAmt"].round(2),
        "tipo": df["ProductCD"],
        "region": df["region"],
        "correo_dominio": df["P_emaildomain"],
    })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datos", default=os.path.join(RAIZ, "data", "ieee-cis"))
    ap.add_argument("--destino", help="raíz del lakehouse a la que se sube todo, p. ej. s3a://fraudlens-lake-216408407230")
    a = ap.parse_args()

    df = cargar(a.datos)
    info_particion = particion(df)
    lista = lista_riesgo(df)
    clientes, comercios = catalogos(df)
    ev = eventos(df)

    sal = os.path.join(a.datos, "preparado")
    os.makedirs(os.path.join(sal, "catalogos"), exist_ok=True)
    archivos = {
        "eventos.csv.gz": lambda p: ev.to_csv(p, index=False, compression="gzip"),
        "etiquetas.csv.gz": lambda p: df[["id_transaccion", "isFraud", "particion"]].to_csv(p, index=False, compression="gzip"),
        "particion.json": lambda p: json.dump(info_particion, open(p, "w", encoding="utf-8"), indent=2, default=str),
        "catalogos/clientes.csv": lambda p: clientes.to_csv(p, index=False),
        "catalogos/comercios.csv": lambda p: comercios.to_csv(p, index=False),
        "riesgo_v%03d.csv" % VERSION_LISTA: lambda p: lista.to_csv(p, index=False),
    }
    for nombre, escribir in archivos.items():
        escribir(os.path.join(sal, nombre))

    prueba = df[df["particion"] == "prueba"]
    alertas_lista = prueba["DeviceInfo"].isin(lista["id_entidad"])
    resumen = {
        "transacciones": len(df), "clientes": len(clientes), "con_dispositivo": int(df["DeviceInfo"].notna().sum()),
        "particion": info_particion,
        "lista_riesgo": {
            "version": VERSION_LISTA, "dispositivos": len(lista),
            "criterio": "al menos %d transacciones y %.0f %% o más de fraude en entrenamiento" % (LISTA_MIN_TX, 100 * LISTA_MIN_TASA),
            "en_prueba": {"alertas": int(alertas_lista.sum()),
                          "precision": round(float(prueba.loc[alertas_lista, "isFraud"].mean()), 4) if alertas_lista.any() else None,
                          "recall": round(float(prueba.loc[alertas_lista, "isFraud"].sum() / prueba["isFraud"].sum()), 4)},
        },
    }
    json.dump(resumen, open(os.path.join(sal, "resumen.json"), "w", encoding="utf-8"), indent=2, ensure_ascii=False, default=str)
    print(json.dumps(resumen, indent=2, ensure_ascii=False, default=str))

    if a.destino:
        from common import lake

        raiz = a.destino.rstrip("/")
        for nombre in list(archivos) + ["resumen.json"]:
            uri = "%s/raw/ieee-cis/preparado/%s" % (raiz, nombre)
            if nombre.startswith("riesgo_v"):
                uri = "%s/landing/listas_riesgo/%s" % (raiz, nombre)
            lake.subir_archivo(os.path.join(sal, nombre), uri)
            print("subido:", uri)


if __name__ == "__main__":
    main()

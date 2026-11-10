"""Replayer de IEEE-CIS hacia Kafka (tarea B3): reproduce transacciones reales en su orden y con su ritmo real,
acelerado por un factor.

Lee eventos.csv.gz (lo genera scripts/preparar_ieee.py) desde el lakehouse o desde un archivo local y publica cada
transacción en el topic `transacciones` con clave id_cliente. La etiqueta isFraud NUNCA se envía: queda aparte para
evaluar (etiquetas.csv.gz).

  # 20.000 transacciones de la partición de prueba, con el tiempo real acelerado 1.300 veces (~50 eventos/s)
  python -m producer.replayer --particion prueba --limite 20000 --factor 1300

  # lo más rápido posible, con tope de 500 eventos/s, y 1 % de registros dañados para probar DLQ y cuarentena
  python -m producer.replayer --factor 0 --tasa-max 500 --limite 50000 --malos-pct 1

  # demo en vivo con el histórico ya cargado: 2.000 pagos reales con un id nuevo, para que el sistema los procese
  # como pagos que llegan hoy (sin --sufijo, Silver los descarta por repetidos: eso demuestra la idempotencia)
  python -m producer.replayer --particion prueba --saltar 100000 --limite 2000 --factor 0 --tasa-max 50 --sufijo demo1

Cada evento lleva ts_evento (hora "histórica": FECHA_BASE + TransactionDT) y ts_emision (hora real de envío). La
latencia del sistema se mide con ts_emision; ts_evento es la que usan las reglas (ventanas de R1, promedio de R2).
Al terminar imprime un resumen JSON (y lo guarda con --resumen) con enviados, errores, duración y rango de fechas.
"""
import argparse
import csv
import gzip
import io
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

from common import config
from producer.producer import Emisor

FECHA_BASE = datetime(2026, 5, 1, tzinfo=timezone.utc)   # la misma de scripts/preparar_ieee.py
ORIGEN_DEFECTO = "raw/ieee-cis/preparado/eventos.csv.gz"


def iso(dt):
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def abrir(origen):
    """Devuelve un lector de texto del CSV gzip, sea ruta local o URI del lakehouse (s3a://, s3://)."""
    if origen.startswith(("s3a://", "s3://")):
        import boto3

        bucket, clave = origen.split("://", 1)[1].split("/", 1)
        s3 = boto3.client("s3", region_name=config.REGION, endpoint_url=config.S3_ENDPOINT)
        cuerpo = s3.get_object(Bucket=bucket, Key=clave)["Body"]
        return io.TextIOWrapper(gzip.GzipFile(fileobj=cuerpo), encoding="utf-8")
    return io.TextIOWrapper(gzip.open(origen), encoding="utf-8")


def a_evento(fila, sufijo=None):
    """Convierte una fila de eventos.csv.gz al contrato del evento. Con `sufijo`, el id de la transacción cambia a
    <id>-<sufijo>: el mismo pago real entra como uno nuevo (para la demo, cuando el histórico ya está cargado). Esos
    pagos no tienen etiqueta, así que no cuentan en la medición del acierto (P7)."""
    vacio = lambda v: v if v not in ("", None) else None  # noqa: E731
    return {
        "id_transaccion": fila["id_transaccion"] + ("-" + sufijo if sufijo else ""),
        "ts_evento": iso(FECHA_BASE + timedelta(seconds=float(fila["transaction_dt"]))),
        "ts_emision": iso(datetime.now(timezone.utc)),
        "id_cliente": fila["id_cliente"],
        "id_comercio": fila["id_comercio"],
        "id_dispositivo": vacio(fila["id_dispositivo"]),
        "monto": float(fila["monto"]),
        "tipo": fila["tipo"],
        "ciudad": vacio(fila["region"]),
        "region": vacio(fila["region"]),
        "correo_dominio": vacio(fila["correo_dominio"]),
    }


def danar(evento, rnd):
    """Un registro malo, para probar la DLQ (JSON ilegible) y la cuarentena (campo inválido)."""
    caso = rnd.choice(["ilegible", "monto", "tipo", "cliente"])
    if caso == "ilegible":
        return "{registro dañado " + evento["id_transaccion"], "ilegible"
    malo = dict(evento, id_transaccion=evento["id_transaccion"] + "-malo")
    if caso == "monto":
        malo["monto"] = -abs(malo["monto"])
    elif caso == "tipo":
        malo["tipo"] = "Z"
    else:
        malo["id_cliente"] = None
    return json.dumps(malo, ensure_ascii=False), "cuarentena"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--origen", default=config.lake(ORIGEN_DEFECTO), help="eventos.csv.gz: ruta local o URI del lake")
    ap.add_argument("--particion", choices=["entrenamiento", "prueba", "todo"], default="prueba")
    ap.add_argument("--saltar", type=int, default=0, help="filas de la partición que se saltan al inicio")
    ap.add_argument("--limite", type=int, default=0, help="máximo de transacciones a enviar (0 = todas)")
    ap.add_argument("--factor", type=float, default=1300.0,
                    help="aceleración del tiempo real (1300 ≈ 50 eventos/s); 0 = sin esperas, solo el tope de --tasa-max")
    ap.add_argument("--tasa-max", type=float, default=500.0, help="tope de eventos por segundo")
    ap.add_argument("--malos-pct", type=float, default=0.0, help="porcentaje de registros dañados que se inyectan")
    ap.add_argument("--semilla", type=int, default=42)
    ap.add_argument("--sufijo", help="se agrega al id de cada transacción para enviarla como un pago nuevo (demo)")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    ap.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "transacciones"))
    ap.add_argument("--resumen", help="archivo donde se guarda el resumen JSON de la corrida")
    a = ap.parse_args()

    rnd = random.Random(a.semilla)
    em = Emisor(a.bootstrap, a.topic)
    intervalo_min = 1.0 / a.tasa_max if a.tasa_max > 0 else 0.0
    enviados, saltadas = 0, 0
    malos = {"ilegible": 0, "cuarentena": 0}
    primero = ultimo = None
    dt0 = None
    t_inicio = time.monotonic()
    proximo_min = t_inicio

    try:
        for fila in csv.DictReader(abrir(a.origen)):
            if a.particion != "todo" and fila["particion"] != a.particion:
                continue
            if saltadas < a.saltar:
                saltadas += 1
                continue
            dt = float(fila["transaction_dt"])
            if dt0 is None:
                dt0 = dt
            # Espera hasta la hora que le toca según el ritmo real acelerado, sin pasar el tope de eventos/s.
            objetivo = t_inicio + ((dt - dt0) / a.factor if a.factor > 0 else 0.0)
            espera = max(objetivo, proximo_min) - time.monotonic()
            if espera > 0:
                time.sleep(espera)
            proximo_min = time.monotonic() + intervalo_min

            evento = a_evento(fila, a.sufijo)
            em.evento(evento)
            enviados += 1
            primero = primero or evento["ts_evento"]
            ultimo = evento["ts_evento"]
            if a.malos_pct and rnd.random() * 100 < a.malos_pct:
                texto, tipo = danar(evento, rnd)
                em.enviar("malformado" if tipo == "ilegible" else evento["id_cliente"], texto)
                malos[tipo] += 1
            if enviados % 10000 == 0:
                print("enviadas %d (ts_evento %s, %.0f ev/s)" % (enviados, ultimo, enviados / (time.monotonic() - t_inicio)),
                      flush=True)
            if a.limite and enviados >= a.limite:
                break
    except KeyboardInterrupt:
        print("interrumpido por el usuario", file=sys.stderr)

    pendientes = em.cerrar()
    duracion = time.monotonic() - t_inicio
    resumen = {
        "origen": a.origen, "particion": a.particion, "factor": a.factor, "tasa_max": a.tasa_max, "sufijo": a.sufijo,
        "enviados": enviados, "malos_inyectados": malos, "entregados_ok": em.ok, "errores_entrega": em.error,
        "pendientes": pendientes, "duracion_s": round(duracion, 1),
        "eventos_por_s": round(enviados / duracion, 1) if duracion else None,
        "ts_evento_primero": primero, "ts_evento_ultimo": ultimo,
    }
    print(json.dumps(resumen, ensure_ascii=False, indent=2))
    if a.resumen:
        with open(a.resumen, "w", encoding="utf-8") as f:
            json.dump(resumen, f, ensure_ascii=False, indent=2)
    return 0 if em.error == 0 and pendientes == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

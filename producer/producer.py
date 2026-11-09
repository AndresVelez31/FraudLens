"""Productor de transacciones sintéticas hacia Kafka (RF-01).

Publica JSON en el topic `transacciones` con clave `id_cliente` (todos los eventos de un cliente van a la misma
partición y conservan su orden), `acks=all` e idempotencia. Cada evento lleva `ts_evento`, la hora en que se generó.

Escenarios (cada uno escribe en --verdad las transacciones inyectadas, para comprobar después las alertas):
  normal           tráfico de fondo (default)
  r1               ráfaga: un cliente supera 5 transacciones en menos de un minuto
  r1-monto         tres pagos grandes que suman más de $2.000.000 en 5 minutos
  r2-base          historial normal de los clientes que luego cometerán R2 (correr ANTES de fraude_pipeline)
  r2               dispositivo nuevo y monto 6 veces mayor al habitual (correr DESPUÉS de fraude_pipeline)
  r3               pagos a comercios y desde dispositivos de la lista de riesgo
  duplicados       reenvía eventos ya enviados (mismo id_transaccion)
  malformados      JSON roto, monto negativo, campo vacío y tipo inválido
  mixto            tráfico normal + una ráfaga R1 y un lote de registros malos cada cierto tiempo

Ejemplos:
  python -m producer.producer --rate 50 --duracion 60
  python -m producer.producer --escenario r1 --verdad /tmp/verdad.jsonl
"""
import argparse
import json
import math
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from producer import catalogo as cat

TIPOS = ["QR", "TRANSFERENCIA", "COMERCIO"]


def ahora_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Generador:
    def __init__(self, n_clientes, n_comercios, semilla):
        self.n_clientes = n_clientes
        self.n_comercios = n_comercios
        self.rnd = random.Random(semilla)
        self.enviados = []  # últimos eventos, para el escenario de duplicados

    def evento(self, i=None, comercio=None, dispositivo=None, monto=None, tipo=None, ts=None):
        r = self.rnd
        i = r.randrange(self.n_clientes) if i is None else i
        c = cat.cliente(i)
        medio = cat.monto_medio(i)
        e = {
            "id_transaccion": str(uuid.uuid4()),  # único entre ejecuciones; la semilla solo fija clientes y escenarios
            "ts_evento": ts or ahora_iso(),
            "id_cliente": c["id_cliente"],
            "id_comercio": comercio or cat.comercio(r.randrange(self.n_comercios))["id_comercio"],
            "id_dispositivo": dispositivo or cat.id_dispositivo(i, r.randrange(cat.n_dispositivos(i))),
            "monto": round(monto if monto is not None else min(400_000.0, r.lognormvariate(math.log(medio), 0.35)), 2),
            "tipo": tipo or r.choice(TIPOS),
            "ciudad": c["ciudad"],
            "documento": c["documento"],
            "telefono": c["telefono"],
            "correo": c["correo"],
        }
        self.enviados.append(e)
        del self.enviados[:-500]
        return e

    # ---- escenarios: devuelven (lista de textos JSON, descripción de la verdad) ----
    def r1(self, n=8):
        i = self.rnd.randrange(self.n_clientes)
        evs = [self.evento(i, monto=120_000.0) for _ in range(n)]
        return evs, {"escenario": "r1", "id_cliente": cat.id_cliente(i), "ids": [e["id_transaccion"] for e in evs]}

    def r1_monto(self):
        i = self.rnd.randrange(self.n_clientes)
        evs = [self.evento(i, monto=900_000.0) for _ in range(3)]
        return evs, {"escenario": "r1-monto", "id_cliente": cat.id_cliente(i), "ids": [e["id_transaccion"] for e in evs]}

    def actores_r2(self, n=5):
        # Clientes de monto habitual bajo (<= 100.000) para que 6x quede por debajo del umbral de R1 de $2M.
        r = random.Random(4242)
        bajos = [i for i in r.sample(range(self.n_clientes), min(self.n_clientes, 5000)) if cat.monto_medio(i) <= 100_000]
        return bajos[:n]

    def r2_base(self):
        """Historial: una compra por día durante los 10 días anteriores, siempre desde el mismo dispositivo.
        Llevan fecha pasada para que no formen una ráfaga (R1) y para que el promedio de 30 días tenga base."""
        evs = []
        ahora = datetime.now(timezone.utc)
        for i in self.actores_r2():
            for dias_atras in range(1, 11):
                ts = (ahora - timedelta(days=dias_atras, hours=self.rnd.randrange(0, 5))).isoformat(timespec="milliseconds")
                evs.append(self.evento(i, dispositivo=cat.id_dispositivo(i, 0), monto=float(cat.monto_medio(i)), ts=ts.replace("+00:00", "Z")))
        return evs, {"escenario": "r2-base", "ids": [e["id_transaccion"] for e in evs]}

    def r2(self):
        evs, verdad = [], []
        for i in self.actores_r2():
            e = self.evento(i, dispositivo="DNUEVO-%07d" % i, monto=float(6 * cat.monto_medio(i)))
            evs.append(e)
            verdad.append(e["id_transaccion"])
        return evs, {"escenario": "r2", "ids": verdad}

    def r3(self):
        evs = []
        i = self.rnd.randrange(self.n_clientes)
        for m in cat.comercios_riesgo():
            evs.append(self.evento(i, comercio=m))
        for d in cat.dispositivos_riesgo():
            evs.append(self.evento(i, dispositivo=d))
        return evs, {"escenario": "r3", "id_cliente": cat.id_cliente(i), "ids": [e["id_transaccion"] for e in evs]}

    def malformados(self):
        base = self.evento()
        textos = [
            "{esto no es json",
            json.dumps(dict(self.evento(), monto=-100.0)),
            json.dumps(dict(self.evento(), id_cliente=None)),
            json.dumps(dict(self.evento(), tipo="CRIPTO")),
            json.dumps(dict(self.evento(), ts_evento="ayer")),
        ]
        del base
        return textos, {"escenario": "malformados", "ilegibles": 1, "cuarentena": 4}


def conectar(bootstrap):
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,  # sin duplicados por reintentos del productor
            "acks": "all",               # con min.insync.replicas=2 del broker: no se pierde un pago confirmado
            "compression.type": "lz4",
            "linger.ms": 5,
            "client.id": "fraudlens-productor",
        }
    )


class Emisor:
    def __init__(self, bootstrap, topic):
        self.p, self.topic = conectar(bootstrap), topic
        self.ok = self.error = 0

    def _cb(self, err, msg):
        if err is not None:
            self.error += 1
            print("ERROR de entrega:", err, file=sys.stderr)
        else:
            self.ok += 1

    def enviar(self, clave, texto):
        while True:
            try:
                self.p.produce(self.topic, key=clave, value=texto, on_delivery=self._cb)
                break
            except BufferError:
                self.p.poll(0.2)
        self.p.poll(0)

    def evento(self, e):
        self.enviar(e["id_cliente"], json.dumps(e, ensure_ascii=False))

    def cerrar(self):
        pendientes = self.p.flush(30)
        return pendientes


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    ap.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "transacciones"))
    ap.add_argument("--rate", type=float, default=50.0, help="eventos por segundo de tráfico normal")
    ap.add_argument("--duracion", type=float, default=60.0, help="segundos de tráfico (escenarios puntuales ignoran esto)")
    ap.add_argument("--escenario", default="mixto",
                    choices=["normal", "mixto", "r1", "r1-monto", "r2-base", "r2", "r3", "duplicados", "malformados"])
    ap.add_argument("--clientes", type=int, default=int(os.environ.get("N_CLIENTES", 20_000)))
    ap.add_argument("--comercios", type=int, default=int(os.environ.get("N_COMERCIOS", 300)))
    ap.add_argument("--semilla", type=int, default=42)
    ap.add_argument("--verdad", help="archivo JSONL donde se anotan las transacciones inyectadas")
    a = ap.parse_args()

    g = Generador(a.clientes, a.comercios, a.semilla)
    em = Emisor(a.bootstrap, a.topic)
    verdad = open(a.verdad, "a", encoding="utf-8") if a.verdad else None

    def inyectar(evs, info):
        for e in evs:
            if isinstance(e, str):  # texto ya armado (JSON roto): no tiene id_cliente que usar como clave
                em.enviar("malformado", e)
            else:
                em.evento(e)
        if verdad:
            verdad.write(json.dumps(info, ensure_ascii=False) + "\n")
            verdad.flush()
        print("inyectado:", {k: (v if k != "ids" else len(v)) for k, v in info.items()})

    puntuales = {"r1": g.r1, "r1-monto": g.r1_monto, "r2-base": g.r2_base, "r2": g.r2, "r3": g.r3, "malformados": g.malformados}
    if a.escenario in puntuales:
        evs, info = puntuales[a.escenario]()
        inyectar(evs, info)
    elif a.escenario == "duplicados":
        for _ in range(3):
            g.evento()
        copias = g.enviados[-3:]
        for e in copias:
            em.evento(e)
        if verdad:
            verdad.write(json.dumps({"escenario": "duplicados", "ids": [e["id_transaccion"] for e in copias]}) + "\n")
        print("reenviados %d eventos (mismo id_transaccion)" % len(copias))
    else:
        t_fin = time.monotonic() + a.duracion
        paso = 0.1
        por_paso = a.rate * paso
        acum, siguiente, n, ultimo_mixto = 0.0, time.monotonic(), 0, time.monotonic()
        while time.monotonic() < t_fin:
            acum += por_paso
            while acum >= 1:
                em.evento(g.evento())
                acum -= 1
                n += 1
            if a.escenario == "mixto" and time.monotonic() - ultimo_mixto > 20:
                inyectar(*g.r1())
                inyectar(*g.malformados())
                ultimo_mixto = time.monotonic()
            siguiente += paso
            time.sleep(max(0.0, siguiente - time.monotonic()))
        print("tráfico normal: %d eventos en %.0fs (%.0f ev/s)" % (n, a.duracion, n / a.duracion))

    pendientes = em.cerrar()
    print("entregados=%d errores=%d pendientes=%d" % (em.ok, em.error, pendientes))
    return 0 if em.error == 0 and pendientes == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

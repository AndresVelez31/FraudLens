"""Catálogo sintético y determinista (misma semilla = mismos clientes, comercios y listas).

Los clientes se derivan del índice sin materializarlos (así el productor escala a millones de clientes en la
prueba de carga) y solo se escriben cuando se siembra PostgreSQL o se exportan CSV.

Uso:
  python -m producer.catalogo --clientes 20000 --comercios 300 --postgres     # siembra PostgreSQL (JDBC lo extrae)
  python -m producer.catalogo --clientes 20000 --csv-dir /ruta/landing/catalogos_csv
  python -m producer.catalogo --lista-riesgo 1 --destino /ruta/landing/listas_riesgo
"""
import argparse
import csv
import os
import random

CIUDADES = ["Medellín", "Bogotá", "Cali", "Barranquilla", "Cartagena", "Bucaramanga", "Pereira"]
SEGMENTOS = ["basico", "plus", "premium"]
CATEGORIAS = ["supermercado", "restaurante", "transporte", "farmacia", "ropa", "tecnología", "servicios", "entretenimiento"]

# Comercios y dispositivos que SOLO aparecen en la lista de riesgo: el tráfico normal nunca los usa, así que
# cualquier pago a ellos es un caso de R3 con respuesta conocida.
N_COMERCIOS_RIESGO = 10
N_DISPOSITIVOS_RIESGO = 10


def id_cliente(i):
    return "C%07d" % i


def id_dispositivo(i, k):
    return "D%07d-%d" % (i, k)


def n_dispositivos(i):
    return 1 + (i % 2)  # 1 o 2 dispositivos por cliente


def monto_medio(i):
    return 40_000 + (i * 7919) % 260_000  # entre 40.000 y 300.000 COP, estable por cliente


def cliente(i):
    return {
        "id_cliente": id_cliente(i),
        "documento": str(1_000_000_000 + (i * 7919) % 900_000_000),
        "telefono": "3%09d" % ((i * 104729) % 1_000_000_000),
        "correo": "cliente%d@correo.co" % i,
        "ciudad": CIUDADES[i % len(CIUDADES)],
        "segmento": SEGMENTOS[i % len(SEGMENTOS)],
    }


def comercio(j):
    return {
        "id_comercio": "M%05d" % j,
        "ciudad": CIUDADES[(j * 3) % len(CIUDADES)],
        "categoria": CATEGORIAS[j % len(CATEGORIAS)],
    }


def comercios_riesgo():
    return ["MR%03d" % k for k in range(N_COMERCIOS_RIESGO)]


def dispositivos_riesgo():
    return ["DR%03d" % k for k in range(N_DISPOSITIVOS_RIESGO)]


def filas_clientes(n):
    try:
        from faker import Faker

        fk = Faker("es_CO")
        Faker.seed(42)
        nombre = fk.name
    except ImportError:  # el catálogo sirve igual sin nombres bonitos
        def nombre():
            return "Cliente"
    for i in range(n):
        c = cliente(i)
        c["nombre"] = nombre()
        c["fecha_alta"] = "2024-%02d-%02d" % (1 + i % 12, 1 + i % 28)
        yield c


def filas_comercios(n):
    try:
        from faker import Faker

        fk = Faker("es_CO")
        Faker.seed(7)
        nombre = fk.company
    except ImportError:
        def nombre():
            return "Comercio"
    for j in range(n):
        c = comercio(j)
        c["nombre"] = nombre()
        yield c
    for idx, mid in enumerate(comercios_riesgo()):
        yield {"id_comercio": mid, "ciudad": CIUDADES[idx % len(CIUDADES)], "categoria": "servicios", "nombre": "Comercio %s" % mid}


COLS_CLIENTES = ["id_cliente", "nombre", "documento", "telefono", "correo", "ciudad", "segmento", "fecha_alta"]
COLS_COMERCIOS = ["id_comercio", "nombre", "ciudad", "categoria"]


def lista_riesgo(version, semilla=42):
    """Versión N de la lista (cada versión es la lista completa y reemplaza a la anterior).

    v1 trae la mitad de los comercios y dispositivos de riesgo; cada versión siguiente suma más. Así se puede
    demostrar que subir un CSV nuevo cambia lo que dispara R3 sin reiniciar nada."""
    com, dis = comercios_riesgo(), dispositivos_riesgo()
    cuantos_c = min(len(com), 5 * version)
    cuantos_d = min(len(dis), 5 * version)
    filas = [("COMERCIO", c, "contracargos reiterados") for c in com[:cuantos_c]]
    filas += [("DISPOSITIVO", d, "dispositivo reportado como robado") for d in dis[:cuantos_d]]
    return filas


def escribir_csv(ruta, columnas, filas):
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(columnas)
        for fila in filas:
            w.writerow([fila[c] for c in columnas] if isinstance(fila, dict) else fila)


def sembrar_postgres(n_clientes, n_comercios):
    import psycopg2
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"), port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "fraudlens"), user=os.environ.get("POSTGRES_USER", "fraudlens"),
        password=os.environ["POSTGRES_PASSWORD"],
    )
    with conn, conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO clientes (%s) VALUES %%s ON CONFLICT (id_cliente) DO UPDATE SET ciudad = EXCLUDED.ciudad,"
            " segmento = EXCLUDED.segmento, actualizado_ts = now()" % ", ".join(COLS_CLIENTES),
            [tuple(c[k] for k in COLS_CLIENTES) for c in filas_clientes(n_clientes)],
            page_size=2000,
        )
        execute_values(
            cur,
            "INSERT INTO comercios (%s) VALUES %%s ON CONFLICT (id_comercio) DO NOTHING" % ", ".join(COLS_COMERCIOS),
            [tuple(c[k] for k in COLS_COMERCIOS) for c in filas_comercios(n_comercios)],
            page_size=2000,
        )
    print("PostgreSQL sembrado: %d clientes, %d comercios" % (n_clientes, n_comercios + N_COMERCIOS_RIESGO))


def cambiar_atributos(n):
    """Cambia ciudad y segmento de n clientes en PostgreSQL: la siguiente carga genera versiones nuevas (SCD2)."""
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"), port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "fraudlens"), user=os.environ.get("POSTGRES_USER", "fraudlens"),
        password=os.environ["POSTGRES_PASSWORD"],
    )
    with conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE clientes SET ciudad = 'Bogotá', segmento = 'premium', actualizado_ts = now()"
            " WHERE id_cliente IN (SELECT id_cliente FROM clientes ORDER BY id_cliente LIMIT %s)", (n,)
        )
    print("%d clientes cambiaron de ciudad y segmento" % n)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clientes", type=int, default=20_000)
    ap.add_argument("--comercios", type=int, default=300)
    ap.add_argument("--postgres", action="store_true", help="siembra PostgreSQL (usa POSTGRES_* del entorno)")
    ap.add_argument("--cambiar-clientes", type=int, default=0, help="modifica N clientes en PostgreSQL (demo de SCD2)")
    ap.add_argument("--csv-dir", help="exporta clientes.csv y comercios.csv (modo CSV de catalogos_diario)")
    ap.add_argument("--lista-riesgo", type=int, help="escribe riesgo_vNNN.csv con esa versión")
    ap.add_argument("--destino", help="carpeta destino de la lista de riesgo")
    a = ap.parse_args()
    if a.postgres:
        sembrar_postgres(a.clientes, a.comercios)
    if a.cambiar_clientes:
        cambiar_atributos(a.cambiar_clientes)
    if a.csv_dir:
        escribir_csv(os.path.join(a.csv_dir, "clientes.csv"), COLS_CLIENTES, filas_clientes(a.clientes))
        escribir_csv(os.path.join(a.csv_dir, "comercios.csv"), COLS_COMERCIOS, filas_comercios(a.comercios))
        print("CSV de catálogos en", a.csv_dir)
    if a.lista_riesgo:
        destino = a.destino or "."
        ruta = os.path.join(destino, "riesgo_v%03d.csv" % a.lista_riesgo)
        escribir_csv(ruta, ["tipo", "id_entidad", "motivo"], lista_riesgo(a.lista_riesgo))
        print("Lista de riesgo escrita en", ruta)


if __name__ == "__main__":
    main()

"""Reglas de fraude R1-R3 como funciones puras sobre DataFrames.

Las usan dos programas distintos y por eso viven aquí:
  * streaming/job.py las aplica a cada micro-batch (R1 sobre ventanas con estado, R2 y R3 con join stream-static),
  * batch/reproceso.py las recalcula sobre rangos de Silver con otra `rule_version` (RF-16).
Cada alerta tiene un `id_alerta` determinista (hash de regla + claves), de modo que reprocesar un lote no la duplica.
"""
import json

from pyspark.sql import Window
from pyspark.sql import functions as F

# Versiones de reglas. `v1` es la del E1; `v2` es un ejemplo más estricto para probar el reproceso (RF-16).
RULES = {
    "v1": {"r1_max_tx": 5, "r1_max_monto": 2_000_000, "r2_factor": 3.0, "r2_dias": 30},
    "v2": {"r1_max_tx": 4, "r1_max_monto": 1_500_000, "r2_factor": 2.5, "r2_dias": 30},
}

# Contenido de dim_regla en Gold.
DIM_REGLA = [
    ("R1", "Ráfaga en ventana de 5 minutos", "Más de N transacciones o más de $X acumulados por cliente en 5 min"),
    ("R2", "Dispositivo nuevo y monto inusual", "Dispositivo nunca visto para el cliente y monto > k veces su promedio de 30 días"),
    ("R3", "Lista de riesgo", "Comercio o dispositivo en la versión vigente de la lista de riesgo"),
]

ALERTA_COLS = [
    "id_alerta", "regla", "rule_version", "id_transaccion", "id_cliente", "id_comercio", "ts_evento",
    "ventana_inicio", "ventana_fin", "n_tx", "monto_total", "detalle", "kafka_partition", "kafka_offset",
]


def config_version(version):
    if version not in RULES:
        raise ValueError("rule_version desconocida: %s (disponibles: %s)" % (version, sorted(RULES)))
    return RULES[version]


def _id(regla, *columnas):
    """Clave determinista de la alerta: misma regla + mismas claves = mismo id (idempotencia)."""
    return F.sha2(F.concat_ws("|", F.lit(regla), *[F.col(c).cast("string") if isinstance(c, str) else c for c in columnas]), 256)


def _nulo(tipo):
    return F.lit(None).cast(tipo)


def _forma(df):
    return df.select(*ALERTA_COLS)


# --------------------------------------------------------------------------------------------------
# R1: ráfaga por cliente en ventana deslizante de 5 min (avanza cada minuto)
# --------------------------------------------------------------------------------------------------
def r1_agregar(tx):
    """Conteo y monto por cliente y ventana. `tx` necesita ts_evento, id_cliente y monto."""
    return tx.groupBy(F.window("ts_evento", "5 minutes", "1 minute").alias("w"), "id_cliente").agg(
        F.count("*").alias("n_tx"), F.sum("monto").alias("monto_total"), F.max("ts_evento").alias("ts_ultimo")
    )


def r1_alertas(agregado, version):
    c = config_version(version)
    detalle = json.dumps({"umbral_tx": c["r1_max_tx"], "umbral_monto": c["r1_max_monto"]})
    f = agregado.where((F.col("n_tx") > c["r1_max_tx"]) | (F.col("monto_total") > c["r1_max_monto"]))
    return _forma(
        f.withColumn("id_alerta", _id("R1", "id_cliente", F.col("w.start")))
        .withColumn("regla", F.lit("R1"))
        .withColumn("rule_version", F.lit(version))
        .withColumn("id_transaccion", _nulo("string"))
        .withColumn("id_comercio", _nulo("string"))
        .withColumn("ts_evento", F.col("ts_ultimo"))
        .withColumn("ventana_inicio", F.col("w.start"))
        .withColumn("ventana_fin", F.col("w.end"))
        .withColumn("detalle", F.lit(detalle))
        .withColumn("kafka_partition", _nulo("int"))
        .withColumn("kafka_offset", _nulo("bigint"))
    )


# --------------------------------------------------------------------------------------------------
# R2: dispositivo nunca visto para el cliente y monto > k veces su promedio de 30 días
# --------------------------------------------------------------------------------------------------
def _alerta_por_transaccion(df, regla, version, detalle_col):
    return _forma(
        df.withColumn("id_alerta", _id(regla, "id_transaccion", *([] if regla == "R2" else ["tipo_lista"])))
        .withColumn("regla", F.lit(regla))
        .withColumn("rule_version", F.lit(version))
        .withColumn("ventana_inicio", _nulo("timestamp"))
        .withColumn("ventana_fin", _nulo("timestamp"))
        .withColumn("n_tx", F.lit(1).cast("bigint"))
        .withColumn("monto_total", F.col("monto"))
        .withColumn("detalle", detalle_col)
    )


def r2_alertas(tx, dispositivos, perfil, version):
    """Versión streaming (stream-static): compara contra el Gold vigente.

    `dispositivos`: (id_cliente, id_dispositivo) ya conocidos. `perfil`: (id_cliente, avg_monto_30d).
    Sin perfil no hay con qué comparar, así que un cliente sin historial no genera R2.
    """
    c = config_version(version)
    vistos = dispositivos.select("id_cliente", "id_dispositivo").distinct().withColumn("_visto", F.lit(1))
    nuevos = tx.join(vistos, ["id_cliente", "id_dispositivo"], "left").where(F.col("_visto").isNull())
    f = nuevos.join(perfil.select("id_cliente", "avg_monto_30d"), "id_cliente", "inner").where(
        F.col("monto") > c["r2_factor"] * F.col("avg_monto_30d")
    )
    detalle = F.to_json(F.struct(F.col("avg_monto_30d"), F.lit(c["r2_factor"]).alias("factor"), F.col("id_dispositivo")))
    return _alerta_por_transaccion(f, "R2", version, detalle)


def r2_alertas_historico(tx_historia, version):
    """Versión batch para reprocesar: calcula 'dispositivo nuevo' y 'promedio de 30 días' como estaban en
    cada momento (as-of) usando solo Silver. `tx_historia` debe incluir los r2_dias previos al rango."""
    c = config_version(version)
    ventana_prev = (
        Window.partitionBy("id_cliente").orderBy(F.col("ts_evento").cast("long")).rangeBetween(-c["r2_dias"] * 86400, -1)
    )
    primera_vez = Window.partitionBy("id_cliente", "id_dispositivo").orderBy("ts_evento", "id_transaccion")
    t = tx_historia.withColumn("avg_monto_30d", F.avg("monto").over(ventana_prev)).withColumn(
        "_orden_disp", F.row_number().over(primera_vez)
    )
    f = t.where(
        (F.col("_orden_disp") == 1)
        & F.col("avg_monto_30d").isNotNull()
        & (F.col("monto") > c["r2_factor"] * F.col("avg_monto_30d"))
    )
    detalle = F.to_json(F.struct(F.col("avg_monto_30d"), F.lit(c["r2_factor"]).alias("factor"), F.col("id_dispositivo")))
    return _alerta_por_transaccion(f, "R2", version, detalle)


# --------------------------------------------------------------------------------------------------
# R3: comercio o dispositivo en la versión vigente de la lista de riesgo
# --------------------------------------------------------------------------------------------------
def lista_vigente(lista_todas):
    """Se queda con la versión más reciente de la lista (cada CSV nuevo reemplaza al anterior)."""
    ultima = lista_todas.agg(F.max("version").alias("v")).collect()[0]["v"]
    return lista_todas.where(F.col("version") == ultima)


def r3_alertas(tx, lista, version):
    """`lista`: (tipo, id_entidad, motivo) de la versión vigente; tipo = COMERCIO | DISPOSITIVO."""
    por_comercio = (
        tx.join(
            lista.where(F.col("tipo") == "COMERCIO").select(F.col("id_entidad").alias("id_comercio"), "motivo"),
            "id_comercio",
        ).withColumn("tipo_lista", F.lit("COMERCIO"))
    )
    por_dispositivo = (
        tx.join(
            lista.where(F.col("tipo") == "DISPOSITIVO").select(F.col("id_entidad").alias("id_dispositivo"), "motivo"),
            "id_dispositivo",
        ).withColumn("tipo_lista", F.lit("DISPOSITIVO"))
    )
    cols = [c for c in por_comercio.columns]
    f = por_comercio.select(*cols).unionByName(por_dispositivo.select(*cols))
    detalle = F.to_json(F.struct(F.col("tipo_lista").alias("lista"), F.col("motivo")))
    return _alerta_por_transaccion(f, "R3", version, detalle)

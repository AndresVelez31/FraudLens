"""Gold con datos históricos (IEEE-CIS): incremental por llegada, perfil de R2 con el reloj de los datos y hora de
Colombia. Corre en la imagen de Spark."""
import json
from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F

from batch import gold
from common import config, tables

AHORA = datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture
def lake(spark, tmp_path, monkeypatch):
    """Lakehouse vacío en una carpeta temporal, con las tablas de Silver creadas."""
    monkeypatch.setattr(config, "LAKE_URI", str(tmp_path))
    for nombre, (_, cols, part) in list(tables.TABLAS.items()):
        capa, tabla = nombre.split("_", 1)
        monkeypatch.setitem(tables.TABLAS, nombre, (config.lake(capa, tabla), cols, part))
    tables.asegurar_tablas(spark)
    return tmp_path


def agregar(spark, filas):
    """filas: (id_transaccion, id_cliente, ts_evento UTC, monto, ingest_ts)."""
    df = spark.createDataFrame(
        [(i, c, ts, m, ing) for i, c, ts, m, ing in filas],
        "id_transaccion string, id_cliente string, ts_evento timestamp, monto double, ingest_ts timestamp",
    )
    cols = spark.read.format("delta").load(tables.ruta("silver_transacciones")).schema
    for campo in cols:
        if campo.name not in df.columns:
            df = df.withColumn(campo.name, F.lit(None).cast(campo.dataType))
    df = (df.withColumn("fecha", F.to_date("ts_evento")).withColumn("id_comercio", F.lit("W"))
          .withColumn("id_dispositivo", F.lit("D1")).withColumn("tipo", F.lit("QR")))
    df.select([c.name for c in cols]).write.format("delta").mode("append").save(tables.ruta("silver_transacciones"))


def gold_ids(spark):
    return {r.id_transaccion for r in spark.read.format("delta").load(gold.ruta("fact_transacciones")).collect()}


def test_incremental_por_llegada_y_no_por_fecha_del_pago(spark, lake):
    mayo = datetime(2026, 5, 10, 12)
    agregar(spark, [("A", "C1", mayo, 10.0, AHORA - timedelta(days=10))])
    gold.hechos(spark, dias=2)                                    # primera vez: todo Silver
    assert gold_ids(spark) == {"A"}
    # B: pago de mayo que llega hoy (la reproducción del histórico). Antes nunca pasaba a Gold.
    # C: llegó hace 10 días y quedó por fuera de esta corrida incremental (ya la habría tomado una corrida anterior).
    agregar(spark, [("B", "C1", mayo + timedelta(days=1), 20.0, AHORA),
                    ("C", "C1", mayo + timedelta(days=2), 30.0, AHORA - timedelta(days=10))])
    gold.hechos(spark, dias=2)
    assert gold_ids(spark) == {"A", "B"}
    gold.hechos(spark, dias=2)                                    # idempotente
    assert spark.read.format("delta").load(gold.ruta("fact_transacciones")).count() == 2


def test_perfil_de_r2_usa_el_reloj_de_los_datos(spark, lake):
    reloj = datetime(2026, 8, 1, 12)                              # muy lejos de la hora real del servidor
    agregar(spark, [("V", "C1", reloj - timedelta(days=40), 1000.0, AHORA),   # fuera de los 30 días
                    ("X", "C1", reloj - timedelta(days=5), 10.0, AHORA),
                    ("Y", "C1", reloj, 30.0, AHORA),
                    ("Z", "C2", reloj - timedelta(days=29), 7.0, AHORA)])
    gold.perfil_cliente(spark)
    p = {r.id_cliente: r for r in spark.read.format("delta").load(gold.ruta("perfil_cliente")).collect()}
    assert p["C1"].avg_monto_30d == 20.0 and p["C1"].n_tx_30d == 2
    assert p["C2"].n_tx_30d == 1 and p["C1"].reloj_datos == reloj


def test_hora_de_colombia_en_id_tiempo_y_dim_tiempo(spark, lake):
    agregar(spark, [("T", "C1", datetime(2026, 5, 10, 3, 30), 5.0, AHORA)])   # 03:30 UTC = 22:30 del 9 en Bogotá
    gold.dim_tiempo(spark)
    gold.hechos(spark, dias=2)
    fila = spark.read.format("delta").load(gold.ruta("fact_transacciones")).collect()[0]
    assert fila.id_tiempo == 2026050922
    dim = {r.id_tiempo: r for r in spark.read.format("delta").load(gold.ruta("dim_tiempo")).collect()}
    assert dim[2026050922].hora == 22 and dim[2026050922].franja == "noche"
    assert str(dim[2026050922].fecha) == "2026-05-09" and dim[2026050922].zona_horaria == "America/Bogota"
    assert len(dim) == 24                                         # el 9 de mayo (hora local) completo


def test_etiquetas_de_fraude_real_y_alertas_con_fecha_del_pago(spark, lake, monkeypatch):
    mayo = datetime(2026, 5, 10, 12)
    agregar(spark, [("F1", "C1", mayo, 100.0, AHORA), ("L1", "C2", mayo, 50.0, AHORA), ("S1", "C3", mayo, 9.0, AHORA)])
    alerta = spark.createDataFrame(
        [("a1", "R4", "ieee", "F1", "C1", "W", mayo, AHORA, 1, 100.0, '{"puntaje": 0.42, "umbral": 0.155}', AHORA),
         ("a2", "R4", "ieee", "L1", "C2", "W", mayo, AHORA, 1, 50.0, '{"puntaje": 0.20, "umbral": 0.155}', AHORA)],
        "id_alerta string, regla string, rule_version string, id_transaccion string, id_cliente string,"
        " id_comercio string, ts_evento timestamp, ts_alerta timestamp, n_tx bigint, monto_total double,"
        " detalle string, ingest_ts timestamp")
    cols = spark.read.format("delta").load(tables.ruta("silver_alertas")).schema
    for c in cols:
        if c.name not in alerta.columns:
            alerta = alerta.withColumn(c.name, F.lit(None).cast(c.dataType))
    alerta.select([c.name for c in cols]).write.format("delta").mode("append").save(tables.ruta("silver_alertas"))
    et = lake / "etiquetas.csv"
    et.write_text("id_transaccion,isFraud,particion\nF1,1,prueba\nL1,0,prueba\nS1,1,entrenamiento\n")
    monkeypatch.setenv("ETIQUETAS_URI", str(et))

    gold.hechos(spark, dias=2)
    tx = {r.id_transaccion: r for r in spark.read.format("delta").load(gold.ruta("fact_transacciones")).collect()}
    assert (tx["F1"].es_sospechosa, tx["F1"].es_fraude_real) == (True, True)       # acierto
    assert (tx["L1"].es_sospechosa, tx["L1"].es_fraude_real) == (True, False)      # falso positivo
    assert (tx["S1"].es_sospechosa, tx["S1"].es_fraude_real) == (False, True)      # fraude no detectado
    assert tx["F1"].particion_eval == "prueba" and tx["S1"].particion_eval == "entrenamiento"
    al = {r.id_alerta: r for r in spark.read.format("delta").load(gold.ruta("fact_alertas")).collect()}
    assert al["a1"].puntaje == 0.42 and al["a1"].es_fraude_real is True and al["a2"].es_fraude_real is False
    assert al["a1"].particion_eval == "prueba"
    assert str(al["a1"].fecha) == "2026-05-10" and al["a1"].id_tiempo == 2026051007   # del pago, en hora de Colombia


def test_sin_archivo_de_etiquetas_no_falla(spark, lake, monkeypatch):
    monkeypatch.setenv("ETIQUETAS_URI", str(lake / "no-existe.csv.gz"))
    agregar(spark, [("A", "C1", datetime(2026, 5, 10, 12), 10.0, AHORA)])
    gold.hechos(spark, dias=2)
    assert spark.read.format("delta").load(gold.ruta("fact_transacciones")).collect()[0].es_fraude_real is None


def test_pago_sin_region_queda_como_sin_region(spark, lake):
    agregar(spark, [("N", "C1", datetime(2026, 5, 10, 12), 10.0, AHORA)])     # ciudad (addr1) vacía
    gold.hechos(spark, dias=2)
    assert spark.read.format("delta").load(gold.ruta("fact_transacciones")).collect()[0].region == "Sin región"


def test_latencia_por_tramo_desde_bronze_y_no_desde_la_fecha_historica(spark, lake):
    mayo = datetime(2026, 5, 10, 12)                                  # fecha histórica del pago (meses atrás)
    emision = (AHORA - timedelta(seconds=100)).replace(microsecond=0)
    kafka, lote, alerta_ts = emision + timedelta(seconds=1), emision + timedelta(seconds=31), emision + timedelta(seconds=41)
    agregar(spark, [("F1", "C1", mayo, 100.0, lote)])
    valor = json.dumps({"id_transaccion": "F1", "ts_emision": emision.isoformat(timespec="milliseconds") + "Z"})
    br = spark.createDataFrame([("F1", valor, kafka, lote)],
                               "id_transaccion string, value string, kafka_timestamp timestamp, ingest_ts timestamp")
    cols = spark.read.format("delta").load(tables.ruta("bronze_transacciones")).schema
    for c in cols:
        if c.name not in br.columns:
            br = br.withColumn(c.name, F.lit(None).cast(c.dataType))
    br.select([c.name for c in cols]).write.format("delta").mode("append").save(tables.ruta("bronze_transacciones"))
    alerta = spark.createDataFrame(
        [("a1", "R4", "ieee", "F1", "C1", "W", mayo, alerta_ts, 1, 100.0, '{"puntaje": 0.4}', lote)],
        "id_alerta string, regla string, rule_version string, id_transaccion string, id_cliente string,"
        " id_comercio string, ts_evento timestamp, ts_alerta timestamp, n_tx bigint, monto_total double,"
        " detalle string, ingest_ts timestamp")
    cols = spark.read.format("delta").load(tables.ruta("silver_alertas")).schema
    for c in cols:
        if c.name not in alerta.columns:
            alerta = alerta.withColumn(c.name, F.lit(None).cast(c.dataType))
    alerta.select([c.name for c in cols]).write.format("delta").mode("append").save(tables.ruta("silver_alertas"))

    gold.hechos(spark, dias=2)
    a = spark.read.format("delta").load(gold.ruta("fact_alertas")).collect()[0]
    assert (a.latencia_s, a.lat_envio_s, a.lat_cola_s, a.lat_reglas_s) == (41.0, 1.0, 30.0, 10.0)


def test_gold_viejo_sin_columnas_de_latencia_se_actualiza(spark, lake):
    agregar(spark, [("A", "C1", datetime(2026, 5, 10, 12), 10.0, AHORA)])
    gold.hechos(spark, dias=2)
    ruta = gold.ruta("fact_alertas")
    viejo = spark.read.format("delta").load(ruta).drop(*gold.COLUMNAS_LATENCIA)   # como estaba antes de D4
    viejo.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(ruta)
    gold.hechos(spark, dias=2)
    assert set(gold.COLUMNAS_LATENCIA) <= set(spark.read.format("delta").load(ruta).columns)

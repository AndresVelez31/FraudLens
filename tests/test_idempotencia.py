"""D1: escrituras del streaming que se pueden repetir sin duplicar (hallazgos 3 y 5). Corren en la imagen de Spark."""
import shutil
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from common import alerts, config, tables


@pytest.fixture
def lake(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "LAKE_URI", str(tmp_path))
    for nombre, (_, cols, part) in list(tables.TABLAS.items()):
        capa, tabla = nombre.split("_", 1)
        monkeypatch.setitem(tables.TABLAS, nombre, (config.lake(capa, tabla), cols, part))
    tables.asegurar_tablas(spark)
    return tmp_path


def test_un_lote_repetido_no_se_escribe_dos_veces(spark, lake):
    df = spark.createDataFrame([("a",), ("b",)], "x string")
    ruta = str(lake / "t")
    tables.escribir_una_vez(df, ruta, "app-1", 7)
    tables.escribir_una_vez(df, ruta, "app-1", 7)          # Spark repite el lote 7 tras una caída
    assert spark.read.format("delta").load(ruta).count() == 2
    tables.escribir_una_vez(df, ruta, "app-1", 8)          # el lote siguiente sí se escribe
    assert spark.read.format("delta").load(ruta).count() == 4


def test_el_id_del_escritor_cambia_si_se_borra_el_checkpoint(lake):
    a = tables.id_de_escritor("ingesta")
    assert tables.id_de_escritor("ingesta") == a           # estable mientras exista el checkpoint
    shutil.rmtree(lake / "checkpoints" / "ingesta")         # los batch_id vuelven a 0...
    assert tables.id_de_escritor("ingesta") != a           # ...y el id cambia: Delta no descarta lo nuevo


class ConcurrentAppendException(Exception):
    pass


def test_reintenta_solo_ante_conflictos_de_delta():
    intentos, esperas = [], []

    def falla_dos_veces():
        intentos.append(1)
        if len(intentos) < 3:
            raise ConcurrentAppendException("Files were added to the root of the table by a concurrent update")
        return "ok"

    assert alerts.con_reintentos(falla_dos_veces, dormir=esperas.append) == "ok"
    assert len(intentos) == 3 and esperas == [1.0, 2.0]

    def otro_error():
        raise ValueError("no es un conflicto")

    with pytest.raises(ValueError):
        alerts.con_reintentos(otro_error, dormir=esperas.append)


def test_merge_de_alertas_por_regla_en_tabla_particionada(spark, lake):
    detalle = spark.sql("DESCRIBE DETAIL delta.`%s`" % tables.ruta("silver_alertas")).collect()[0]
    assert detalle.partitionColumns == ["regla"]
    ts = datetime(2026, 5, 10, 12)
    filas = [("a1", "R4", "T1", 1), ("a2", "R2", "T2", 1), ("a3", "R1", None, 3)]
    df = spark.createDataFrame(filas, "id_alerta string, regla string, id_transaccion string, n_tx bigint")
    cols = spark.read.format("delta").load(tables.ruta("silver_alertas")).schema
    for c in cols:
        if c.name not in df.columns and c.name not in ("ts_alerta", "batch_id", "source_system", "ingest_ts"):
            df = df.withColumn(c.name, F.lit(None).cast(c.dataType))
    df = df.withColumn("ts_evento", F.lit(ts))
    alerts.merge_silver(spark, df, ts, 1)
    alerts.merge_silver(spark, df.withColumn("n_tx", F.when(F.col("regla") == "R1", 5).otherwise(1)), ts, 2)
    s = {r.id_alerta: r for r in spark.read.format("delta").load(tables.ruta("silver_alertas")).collect()}
    assert sorted(s) == ["a1", "a2", "a3"] and s["a3"].n_tx == 5        # sin duplicados; R1 actualiza su ventana

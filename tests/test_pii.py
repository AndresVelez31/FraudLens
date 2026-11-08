import os

from common import pii


def test_hmac_nativo_de_spark_coincide_con_la_libreria_estandar(spark):
    clave = os.environ["PII_HMAC_KEY"]
    valores = ["1020304050", "3001234567", "Cliente@Correo.CO", "  con espacios  ", "ñandú", "x" * 200]
    df = spark.createDataFrame([(v,) for v in valores] + [(None,)], "v string")
    filas = df.select("v", pii.hmac_col(df.v).alias("h")).collect()
    for r in filas:
        assert r.h == pii.hmac_hex(r.v, clave)
    assert [r.h for r in filas if r.v is None] == [None]


def test_clave_larga_tambien_coincide(spark, monkeypatch):
    monkeypatch.setenv("PII_HMAC_KEY", "k" * 100)  # más larga que un bloque: se hashea primero
    df = spark.createDataFrame([("abc",)], "v string")
    assert df.select(pii.hmac_col(df.v)).collect()[0][0] == pii.hmac_hex("abc", "k" * 100)

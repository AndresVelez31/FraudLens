"""Pruebas de la lógica pura del streaming: clasificación de registros y reglas R1-R3 (sin Kafka)."""
import json
from datetime import datetime, timedelta

from pyspark.sql import functions as F

from common import rules
from streaming.job import a_silver, clasificar


def evento(i, **kw):
    e = {"id_transaccion": "T%03d" % i, "ts_evento": "2026-11-04T14:00:00.000Z", "id_cliente": "C1", "id_comercio": "M1",
         "id_dispositivo": "D1", "monto": 1000.0, "tipo": "QR", "ciudad": "Medellín",
         "documento": "123", "telefono": "300", "correo": "a@b.co"}
    e.update(kw)
    return e


def raw(spark, textos):
    filas = [(t, "transacciones", 0, i) for i, t in enumerate(textos)]
    return spark.createDataFrame(filas, "value string, kafka_topic string, kafka_partition int, kafka_offset long") \
        .withColumn("ingest_ts", F.current_timestamp())


def test_clasifica_ilegibles_cuarentena_y_validos(spark):
    textos = [
        json.dumps(evento(1)),                              # válido
        "{esto no es json",                                 # ilegible -> DLQ
        json.dumps(evento(3, monto=-5.0)),                  # monto negativo -> cuarentena
        json.dumps(evento(4, id_cliente=None)),             # campo obligatorio vacío -> cuarentena
        json.dumps(evento(5, ts_evento="ayer")),            # fecha inválida -> cuarentena
        json.dumps(evento(6, tipo="CRIPTO")),               # tipo inválido -> cuarentena
    ]
    c = clasificar(raw(spark, textos)).orderBy("kafka_offset").collect()
    estado = [(r.ilegible, r.motivo) for r in c]
    assert estado[0] == (False, None)
    assert estado[1][0] is True
    assert estado[2] == (False, "monto_no_positivo")
    assert estado[3] == (False, "campo_obligatorio_nulo:id_cliente")
    assert estado[4] == (False, "ts_evento_invalido")
    assert estado[5] == (False, "tipo_invalido")


def test_silver_oculta_datos_personales_y_deduplica(spark):
    textos = [json.dumps(evento(1)), json.dumps(evento(1))]  # el mismo id dos veces
    c = clasificar(raw(spark, textos))
    s = a_silver(c.where(~F.col("ilegible") & F.col("motivo").isNull()), "b1")
    filas = s.collect()
    assert len(filas) == 1
    r = filas[0]
    assert r.documento_h != "123" and len(r.documento_h) == 64     # HMAC-SHA-256 en hex
    assert r.telefono_h != "300" and r.correo_h != "a@b.co"
    assert str(r.fecha) == "2026-11-04"


def tx_df(spark, filas):
    return spark.createDataFrame(
        filas, "id_transaccion string, ts_evento timestamp, id_cliente string, id_comercio string,"
               " id_dispositivo string, monto double, tipo string, kafka_partition int, kafka_offset long")


def test_r1_ventana_5min_supera_umbral_de_conteo_y_de_monto(spark):
    t0 = datetime(2026, 11, 4, 14, 0, 0)
    rafaga = [("A%d" % i, t0 + timedelta(seconds=10 * i), "C1", "M1", "D1", 1000.0, "QR", 0, i) for i in range(7)]   # 7 tx
    grandes = [("B%d" % i, t0 + timedelta(seconds=20 * i), "C2", "M1", "D2", 900_000.0, "QR", 0, 100 + i) for i in range(3)]  # $2,7M
    normal = [("N%d" % i, t0 + timedelta(minutes=10 * i), "C3", "M1", "D3", 50_000.0, "QR", 0, 200 + i) for i in range(4)]
    agg = rules.r1_agregar(tx_df(spark, rafaga + grandes + normal))
    clientes = {r.id_cliente for r in rules.r1_alertas(agg, "v1").collect()}
    assert clientes == {"C1", "C2"}      # C3 queda por debajo en toda ventana


def test_r1_id_es_determinista_y_v2_es_mas_estricta(spark):
    t0 = datetime(2026, 11, 4, 14, 0, 0)
    cinco = [("A%d" % i, t0 + timedelta(seconds=10 * i), "C1", "M1", "D1", 1000.0, "QR", 0, i) for i in range(5)]
    agg = rules.r1_agregar(tx_df(spark, cinco))
    assert rules.r1_alertas(agg, "v1").count() == 0           # 5 tx no superan "más de 5"
    v2 = rules.r1_alertas(agg, "v2")
    assert v2.count() > 0                                      # con v2 (más de 4) sí
    ids1 = sorted(r.id_alerta for r in v2.collect())
    ids2 = sorted(r.id_alerta for r in rules.r1_alertas(rules.r1_agregar(tx_df(spark, cinco)), "v2").collect())
    assert ids1 == ids2                                        # mismo dato -> mismos ids


def test_r2_dispositivo_nuevo_y_monto_inusual(spark):
    t = datetime(2026, 11, 4, 14, 0, 0)
    tx = tx_df(spark, [
        ("X1", t, "C1", "M1", "D-NUEVO", 500_000.0, "QR", 0, 1),   # nuevo y 5x el promedio -> alerta
        ("X2", t, "C1", "M1", "D-VIEJO", 500_000.0, "QR", 0, 2),   # dispositivo conocido -> no
        ("X3", t, "C1", "M1", "D-NUEVO2", 120_000.0, "QR", 0, 3),  # nuevo pero monto normal -> no
        ("X4", t, "C9", "M1", "D-NUEVO", 900_000.0, "QR", 0, 4),   # cliente sin perfil -> no
    ])
    disp = spark.createDataFrame([("C1", "D-VIEJO")], "id_cliente string, id_dispositivo string")
    perfil = spark.createDataFrame([("C1", 100_000.0)], "id_cliente string, avg_monto_30d double")
    ids = {r.id_transaccion for r in rules.r2_alertas(tx, disp, perfil, "v1").collect()}
    assert ids == {"X1"}


def test_r3_lista_de_riesgo_vigente(spark):
    t = datetime(2026, 11, 4, 14, 0, 0)
    tx = tx_df(spark, [
        ("Y1", t, "C1", "M-MALO", "D1", 10.0, "QR", 0, 1),
        ("Y2", t, "C1", "M1", "D-MALO", 10.0, "QR", 0, 2),
        ("Y3", t, "C1", "M1", "D1", 10.0, "QR", 0, 3),
        ("Y4", t, "C1", "M-VIEJO", "D1", 10.0, "QR", 0, 4),   # estaba en la versión 1, ya no está en la 2
    ])
    todas = spark.createDataFrame([
        (1, "COMERCIO", "M-VIEJO", "fraude"), (2, "COMERCIO", "M-MALO", "fraude"), (2, "DISPOSITIVO", "D-MALO", "robado"),
    ], "version int, tipo string, id_entidad string, motivo string")
    vigente = rules.lista_vigente(todas)
    ids = {r.id_transaccion for r in rules.r3_alertas(tx, vigente, "v1").collect()}
    assert ids == {"Y1", "Y2"}


def test_r2_ignora_transacciones_sin_dispositivo(spark):
    # Con IEEE-CIS el 80 % de las transacciones no trae dispositivo: no pueden contar como "dispositivo nuevo".
    t = datetime(2026, 11, 4, 14, 0, 0)
    tx = tx_df(spark, [
        ("Z1", t, "C1", "W", None, 900_000.0, "W", 0, 1),        # sin dispositivo y monto alto -> no
        ("Z2", t, "C1", "W", "D-NUEVO", 900_000.0, "W", 0, 2),   # dispositivo nuevo y monto alto -> sí
    ])
    disp = spark.createDataFrame([("C1", "D-VIEJO"), ("C1", None)], "id_cliente string, id_dispositivo string")
    perfil = spark.createDataFrame([("C1", 100_000.0)], "id_cliente string, avg_monto_30d double")
    assert {r.id_transaccion for r in rules.r2_alertas(tx, disp, perfil, "v1").collect()} == {"Z2"}


def test_eventos_de_ieee_pasan_la_validacion(spark):
    # Tipo de producto de IEEE-CIS y sin dispositivo: deben ser válidos, no ir a cuarentena.
    e = evento(1, tipo="C", id_dispositivo=None, id_comercio="C", ciudad="R315")
    c = clasificar(raw(spark, [json.dumps(e)])).collect()[0]
    assert (c.ilegible, c.motivo) == (False, None)


def test_reglas_activas_desde_el_entorno(monkeypatch):
    import importlib

    from common import config

    monkeypatch.setenv("REGLAS_ACTIVAS", "r2, R3,R4")
    c = importlib.reload(config)
    try:
        assert not c.regla_activa("R1") and c.regla_activa("R2") and c.regla_activa("r4")
    finally:
        monkeypatch.delenv("REGLAS_ACTIVAS")
        importlib.reload(config)

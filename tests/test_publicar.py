"""Pruebas de `registrar_glue` (batch/publicar.py) con S3 y Glue simulados por moto.

Athena se reemplaza por un doble que imita lo que se vio en AWS: si la tabla Delta existe, el DDL crea una entrada
sana en Glue (con columnas); si falla, deja una entrada rota (sin columnas) y devuelve FAILED.
"""
import itertools

import boto3
import pytest
from moto import mock_aws

from batch import publicar
from common import config

BUCKET = "lago-prueba"
BD = "fraudlens"


class AthenaFalsa:
    def __init__(self, glue, fallar=()):
        self.glue, self.fallar, self.consultas, self._ids = glue, set(fallar), {}, itertools.count()
        self.columnas = [{"Name": "id", "Type": "string"}]

    def start_query_execution(self, QueryString, WorkGroup):
        tabla = QueryString.split("EXISTS %s." % BD)[1].split()[0]
        qid = str(next(self._ids))
        if tabla in self.fallar:  # como en AWS: falla, pero deja una entrada rota
            self._crear(tabla, columnas=[])
            self.consultas[qid] = ("FAILED", "Delta Table DDL failed with an unexpected error.")
        else:
            self._crear(tabla, columnas=self.columnas)
            self.consultas[qid] = ("SUCCEEDED", None)
        return {"QueryExecutionId": qid}

    def get_query_execution(self, QueryExecutionId):
        st, motivo = self.consultas[QueryExecutionId]
        return {"QueryExecution": {"Status": {"State": st, "StateChangeReason": motivo}}}

    def _crear(self, tabla, columnas):
        sd = {"Columns": columnas, "Location": "s3://%s/gold/%s/" % (BUCKET, tabla)}
        try:
            self.glue.create_table(DatabaseName=BD, TableInput={"Name": tabla, "StorageDescriptor": sd})
        except self.glue.exceptions.AlreadyExistsException:
            pass


@pytest.fixture
def aws(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t", "AWS_DEFAULT_REGION": "us-east-2",
                 "GLUE_DATABASE": BD, "ATHENA_WORKGROUP": "fraudlens"}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setattr(config, "S3_ENDPOINT", None)
    monkeypatch.setattr(config, "LAKE_URI", "s3a://%s" % BUCKET)
    monkeypatch.setattr(config, "REGION", "us-east-2")
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-2")
        s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        glue = boto3.client("glue", region_name="us-east-2")
        glue.create_database(DatabaseInput={"Name": BD})
        yield s3, glue


def crear_delta(s3, *tablas):
    for t in tablas:
        s3.put_object(Bucket=BUCKET, Key="gold/%s/_delta_log/00000000000000000000.json" % t, Body=b"{}")


def test_clasifica_entradas_de_glue():
    assert publicar.estado_en_glue(None) == "falta"
    assert publicar.estado_en_glue({"StorageDescriptor": {"Location": "s3://b/t", "Columns": []}}) == "rota"
    assert publicar.estado_en_glue({"Parameters": {"table_type": "DELTA"}}) == "rota"
    assert publicar.estado_en_glue({"StorageDescriptor": {"Location": "s3://b/t", "Columns": [{"Name": "a"}]}}) == "ok"


def test_salta_las_tablas_que_aun_no_existen_en_s3(aws):
    s3, glue = aws
    crear_delta(s3, "dim_cliente")
    res = publicar.registrar_glue(s3=s3, glue=glue, athena=AthenaFalsa(glue))
    assert res["dim_cliente"] == "registrada"
    assert res["dim_tiempo"].startswith("pendiente")
    with pytest.raises(glue.exceptions.EntityNotFoundException):
        glue.get_table(DatabaseName=BD, Name="dim_tiempo")  # no deja entradas de tablas que no existen


def test_repara_una_entrada_rota_de_un_intento_anterior(aws):
    s3, glue = aws
    crear_delta(s3, "dim_tiempo")
    glue.create_table(DatabaseName=BD, TableInput={"Name": "dim_tiempo", "Parameters": {"table_type": "DELTA"}})
    res = publicar.registrar_glue(s3=s3, glue=glue, athena=AthenaFalsa(glue))
    assert res["dim_tiempo"] == "reparada"
    assert publicar.estado_en_glue(glue.get_table(DatabaseName=BD, Name="dim_tiempo")["Table"]) == "ok"


def test_no_toca_las_tablas_ya_registradas(aws):
    s3, glue = aws
    crear_delta(s3, "dim_cliente")
    athena = AthenaFalsa(glue)
    publicar.registrar_glue(s3=s3, glue=glue, athena=athena)
    res = publicar.registrar_glue(s3=s3, glue=glue, athena=athena)
    assert res["dim_cliente"] == "ya registrada"
    assert len(athena.consultas) == 1  # la segunda corrida no vuelve a ejecutar el DDL


def test_si_una_falla_registra_las_demas_y_no_deja_entradas_rotas(aws):
    s3, glue = aws
    crear_delta(s3, "dim_cliente", "fact_transacciones")
    with pytest.raises(RuntimeError, match="fact_transacciones"):
        publicar.registrar_glue(s3=s3, glue=glue, athena=AthenaFalsa(glue, fallar={"fact_transacciones"}))
    assert publicar.estado_en_glue(glue.get_table(DatabaseName=BD, Name="dim_cliente")["Table"]) == "ok"
    with pytest.raises(glue.exceptions.EntityNotFoundException):
        glue.get_table(DatabaseName=BD, Name="fact_transacciones")  # el reintento la podrá crear de cero


def test_en_aws_sin_configuracion_falla(aws, monkeypatch):
    monkeypatch.delenv("ATHENA_WORKGROUP")
    with pytest.raises(RuntimeError, match="ATHENA_WORKGROUP"):
        publicar.registrar_glue()


def test_en_local_se_omite(monkeypatch):
    monkeypatch.setattr(config, "S3_ENDPOINT", "http://minio:9000")
    assert publicar.registrar_glue() == {}


def test_recrea_la_entrada_si_gold_cambio_de_columnas(aws):
    s3, glue = aws
    crear_delta(s3, "fact_alertas")
    athena = AthenaFalsa(glue)
    publicar.registrar_glue(s3=s3, glue=glue, athena=athena)                      # Glue queda con ["id"]
    athena.columnas = [{"Name": "id", "Type": "string"}, {"Name": "puntaje", "Type": "double"}]
    res = publicar.registrar_glue(s3=s3, glue=glue, athena=athena, esquemas={"fact_alertas": {"id", "puntaje"}})
    assert res["fact_alertas"] == "actualizada"
    assert publicar.columnas_glue(glue.get_table(DatabaseName=BD, Name="fact_alertas")["Table"]) == {"id", "puntaje"}
    res = publicar.registrar_glue(s3=s3, glue=glue, athena=athena, esquemas={"fact_alertas": {"id", "puntaje"}})
    assert res["fact_alertas"] == "ya registrada"                                 # ya coincide: no se toca

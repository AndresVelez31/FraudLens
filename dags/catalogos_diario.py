"""DAG `catalogos_diario` (RF-02, RF-03, RF-15): catálogos de PostgreSQL y listas de riesgo (CSV) -> Bronze -> Silver.

    extract_postgres -> sensor_csv_riesgo -> load_bronze -> silver_catalogos

Corre una vez al día y también cuando `disparar_catalogos_si_csv_nuevo` detecta un CSV de riesgo nuevo en landing.
Todas las tareas son idempotentes: repetir el DAG no duplica datos. Con `all_success`, si una tarea falla las
siguientes no corren (no se propagan datos a medias).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utiles_fraudlens import ARGS_POR_DEFECTO, comando

CSV_RIESGO = "riesgo_v"


def hay_csv_de_riesgo():
    """El sensor espera a que exista al menos una versión de la lista de riesgo en landing/listas_riesgo/."""
    from common import config, lake

    return any(CSV_RIESGO in u for u in lake.listar(config.lake("landing", "listas_riesgo")))


def hay_version_nueva():
    from batch.catalogos import csv_pendientes
    from common.spark import get_spark

    spark = get_spark("fraudlens-sensor-csv")
    try:
        return bool(csv_pendientes(spark))
    finally:
        spark.stop()


with DAG(
    dag_id="catalogos_diario",
    description="PostgreSQL y CSV de riesgo -> Bronze -> Silver (diario o al llegar un CSV)",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=ARGS_POR_DEFECTO,
    dagrun_timeout=timedelta(hours=1),
    tags=["fraudlens", "batch"],
) as catalogos:
    extract_postgres = BashOperator(task_id="extract_postgres", bash_command=comando("batch.catalogos", "extract_batch"))
    sensor_csv_riesgo = PythonSensor(
        task_id="sensor_csv_riesgo", python_callable=hay_csv_de_riesgo, poke_interval=15, timeout=600, mode="reschedule")
    load_bronze = BashOperator(task_id="load_bronze", bash_command=comando("batch.catalogos", "load_bronze"))
    silver_catalogos = BashOperator(task_id="silver_catalogos", bash_command=comando("batch.catalogos", "spark_silver"))
    # Una vez al día se compactan y ordenan los hechos de Gold (OPTIMIZE + ZORDER) para que Athena lea menos.
    optimize_gold = BashOperator(task_id="optimize_gold", bash_command=comando("batch.publicar", "optimize"))
    extract_postgres >> sensor_csv_riesgo >> load_bronze >> silver_catalogos >> optimize_gold


# "Al llegar un CSV": cada 5 minutos mira landing y, si hay una versión sin cargar, lanza catalogos_diario.
with DAG(
    dag_id="disparar_catalogos_si_csv_nuevo",
    description="Detecta una versión nueva de la lista de riesgo y dispara catalogos_diario",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=dict(ARGS_POR_DEFECTO, retries=1),
    tags=["fraudlens", "batch"],
) as vigilante:
    hay_nueva = ShortCircuitOperator(task_id="hay_version_nueva", python_callable=hay_version_nueva)
    disparar = TriggerDagRunOperator(task_id="disparar_catalogos_diario", trigger_dag_id="catalogos_diario")
    hay_nueva >> disparar

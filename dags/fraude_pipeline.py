"""DAG `fraude_pipeline` (RF-10 a RF-15): Silver -> Gold, calidad y registro en Athena, cada 10 minutos.

    check_streaming_vivo -> gold_dimensiones -> gold_hechos -> quality_checks -> registrar_glue

* `check_streaming_vivo` lee el latido del job de streaming y falla si lleva más de 2 minutos sin avanzar.
* Con `all_success`, un fallo en cualquier paso detiene los siguientes (nada llega a Gold a medias).
* Cada tarea reintenta 3 veces con espera exponencial y, si sigue fallando, avisa por SNS.
* Evento -> Gold queda en máximo ~15 min: el DAG corre cada 10 y dura pocos minutos (RNF-04).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utiles_fraudlens import ARGS_POR_DEFECTO, comando

LATIDO_MAXIMO_S = 120


def verificar_streaming():
    from common import lake

    edad = lake.edad_latido()
    if edad is None:
        raise AirflowFailException("El job de streaming nunca publicó latido: no está corriendo")
    if edad > LATIDO_MAXIMO_S:
        raise AirflowFailException("El job de streaming lleva %.0f s sin latido (máximo %d s)" % (edad, LATIDO_MAXIMO_S))
    print("streaming vivo: último latido hace %.0f s" % edad)


with DAG(
    dag_id="fraude_pipeline",
    description="Silver -> Gold (esquema en estrella), calidad y registro en Athena cada 10 minutos",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,  # si una corrida se alarga, la siguiente espera: no se pisan
    default_args=ARGS_POR_DEFECTO,
    dagrun_timeout=timedelta(minutes=9),
    tags=["fraudlens", "gold"],
) as dag:
    check_streaming_vivo = PythonOperator(task_id="check_streaming_vivo", python_callable=verificar_streaming)
    gold_dimensiones = BashOperator(task_id="gold_dimensiones", bash_command=comando("batch.gold", "dimensiones"))
    gold_hechos = BashOperator(task_id="gold_hechos", bash_command=comando("batch.gold", "hechos"))
    quality_checks = BashOperator(task_id="quality_checks", bash_command=comando("batch.calidad"))
    registrar_glue = BashOperator(
        task_id="registrar_glue",
        bash_command=" && ".join([comando("batch.publicar", p) for p in ("optimize", "glue", "metabase")]),
    )
    check_streaming_vivo >> gold_dimensiones >> gold_hechos >> quality_checks >> registrar_glue

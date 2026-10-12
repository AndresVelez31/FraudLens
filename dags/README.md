# dags

**Responsable:** Hellen Yanes Doria · **Par revisor:** Andrés Felipe Vélez Álvarez

DAG de Airflow 2.9 (LocalExecutor): `catalogos_diario` y `fraude_pipeline` (cada 10 min), con dependencias
`all_success`, 3 reintentos con espera exponencial, `check_streaming_vivo` y `quality_checks`.

Requisitos: RF-15.

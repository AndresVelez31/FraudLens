# Demo en vivo (8 minutos) en AWS

La guía del curso pide que en la demo se vean cuatro cosas, y que el procesamiento ocurra frente al docente:
- el productor enviando a Kafka;
- el job de Spark procesando;
- una consulta de Gold que responda una pregunta de negocio del E1;
- el DAG de Airflow con su estado.

Todo se maneja desde el portátil con `scripts/demo_aws.sh` (Git Bash), sin SSH ni llaves.

**Por qué los pagos llevan sufijo.** Los 590.540 pagos reales ya están cargados. Si se reenviaran tal cual, Silver los
descartaría por repetidos; eso sirve para mostrar la idempotencia, pero no genera alertas nuevas. Para la parte en vivo,
el replayer manda pagos reales de la partición de prueba con un sufijo en el id (`--sufijo`): el sistema los procesa
como pagos que llegan hoy. Esos pagos no tienen etiqueta, así que no cambian la medición del acierto (P7).

## El día anterior: ensayo obligatorio
1. `aws login --profile fraudlens` (lo hace quien presenta).
2. `bash scripts/demo_aws.sh encender` y, a los 3 minutos, `bash scripts/demo_aws.sh estado`: en la EC2-A deben estar los
   3 brokers y `streaming`; en la EC2-B, `postgres`, `airflow-scheduler`, `airflow-webserver` y `api`.
3. `bash scripts/demo_aws.sh preparar`: deja `main`, `NOTIFICAR_DESDE=lambda` (los correos los manda solo la Lambda)
   y el DAG `fraude_pipeline` activo.
4. Correr la demo completa con cronómetro (sección siguiente) y anotar cuánto tardó cada paso.
5. `bash scripts/demo_aws.sh apagar`. Costo del ensayo: unos USD 0,20 por hora con las dos EC2.

Si se quiere ver Airflow en el navegador, instalar el Session Manager plugin del AWS CLI (instalador oficial de AWS).
Sin él, `bash scripts/demo_aws.sh gold` muestra el mismo estado del DAG en la terminal.

## El día de la sustentación
- **30 min antes:** `encender`, `estado`. Abrir Power BI (`dashboards/powerbi/FraudLens.pbip`) y el diagrama.
- Tener abiertas dos terminales en la raíz del repo y el correo donde llegan las alertas.

## Guion (8 minutos)
| Min | Qué se hace | Comando | Qué se dice |
|---|---|---|---|
| 0:00 | Estado del sistema | `estado` | «Dos EC2: la A con Kafka y Spark, la B con Airflow, PostgreSQL y la API. Todo en contenedores.» |
| 0:45 | **Productor → Kafka** | `enviar 2000` | «Mandamos 2.000 pagos reales de la partición de prueba, a 50 por segundo, con 1 % de registros dañados.» |
| 1:30 | **Spark procesando** | `streaming` (repetir cada 30 s) | «Cada micro-lote: crudo a Bronze, inválidos a cuarentena, ilegibles a la DLQ, MERGE a Silver y reglas R2, R3 y R4.» |
| 3:00 | Alertas | `alertas R4` y `alertas R3` | «Las alertas nuevas, con el id que lleva el sufijo. Están en DynamoDB, escritas una sola vez.» Mostrar el correo resumen si ya llegó (la Lambda junta hasta 5 min). |
| 4:00 | **DAG de Airflow** | `gold` (o Airflow en el navegador) | «`fraude_pipeline` corre cada 10 min: verifica que el streaming esté vivo, construye Gold, controla la calidad y registra en Glue. Si una tarea falla, las siguientes no corren y llega un correo.» |
| 5:30 | **Pregunta de negocio sobre Gold** | `consulta p6`, luego `consulta p5` | «P6 del E1: las 10 regiones con más monto sospechoso de la última semana, en 3 s y leyendo 5 MB. P5: la latencia por tramo, con los pagos que acabamos de mandar.» |
| 6:30 | Acierto | `consulta p7` o Power BI, página «Acierto contra la realidad» | «Contra el fraude real: 20,9 % de precisión y 23,7 % de cobertura en la partición de prueba.» |
| 7:15 | **Idempotencia** | `repetir 500` y luego `streaming` | «Reenviamos 500 pagos que ya estaban. Bronze los guarda como llegaron, el MERGE no los vuelve a meter en Silver y las reglas los marcan otra vez, pero el log dice "0 nuevas": DynamoDB no acepta una alerta repetida y no sale otro correo.» |

Si sobra tiempo, o el docente lo pide: abrir el código de lo que se pregunte. Las rutas están en
`docs/entregable2/guia_defensa.md`.

## Si algo falla en vivo
La guía del curso dice que un fallo bien explicado puede mantener la nota. Causas probables y qué decir:
| Síntoma | Causa probable | Qué se hace |
|---|---|---|
| `ExpiredToken` o `AccessDenied` en el portátil | La sesión de `aws login` venció | `aws login --profile fraudlens` y repetir el comando |
| `estado` no muestra `postgres` en la EC2-B | PostgreSQL no arrancó con la máquina | Repetir `preparar`; si no, explicar que Airflow depende de PostgreSQL y se levanta con `docker compose up -d` |
| Las alertas tardan más de 3 min | El lote tarda ~68 s porque R4 lee la historia del cliente desde Silver | Es la latencia medida (P95 ~202 s); explicar la causa y el arreglo con una tabla de estado |
| El DAG falla en `check_streaming_vivo` | El streaming no escribió en los últimos minutos | Es el control funcionando: el DAG no construye Gold con datos viejos. `streaming` muestra el estado |
| No llega el correo | La Lambda junta hasta 5 min antes de mandar | Mostrar las alertas en la API; el correo llega después |
| Sin internet en el salón | No se llega a AWS | Mostrar Power BI (modo Import, funciona sin conexión) y el registro del ensayo |

# Demo en vivo (8 minutos) en AWS

La guía del curso pide que en la demo se vean cuatro cosas, y que el procesamiento ocurra frente al docente:
- el productor enviando a Kafka;
- el job de Spark procesando;
- una consulta de Gold que responda una pregunta de negocio del E1;
- el DAG de Airflow con su estado.

Todo se maneja desde el portátil con `scripts/demo_aws.sh` (Git Bash), sin SSH ni llaves.

**Qué pagos se envían.** Los 590.540 pagos reales ya están cargados; si se reenvían, Silver los descarta por repetidos
y no se ve nada nuevo. Por eso, antes de la demo, `reservar` saca del sistema los **últimos 2.000 pagos del dataset**
(de Silver, de las alertas y de DynamoDB) y en la demo `enviar` los manda otra vez. Llegan como la primera vez: con la
historia del cliente que tenían, su etiqueta de fraude y su tasa real de alertas. En el ensayo dieron 70 alertas de
2.000 (3,5 %); la carga original les había dado 71. Gold no se toca: al llegar, el MERGE actualiza sus filas.

## Tiempos medidos en el ensayo
| Paso | Tiempo |
|---|---|
| `encender` hasta que los contenedores están arriba | ~3 min |
| `reservar 2000` | ~2 min |
| `enviar 2000` (a 500 por segundo) | 4 s de envío |
| Micro-lote de Spark con esos pagos, hasta ver las alertas | ~3 min (casi todo es R4 leyendo la historia de los clientes) |
| Correo resumen de la Lambda | hasta 5 min después de la alerta |
| Corrida completa del DAG `fraude_pipeline` | ~7,5 min (corre sola cada 10 min) |
| Consulta de Athena | ~3 s |

Como el micro-lote tarda ~3 min, el guion manda los pagos al principio y llena la espera con el DAG y Athena.

## El día anterior: ensayo obligatorio
1. `aws login --profile fraudlens` (lo hace quien presenta).
2. `bash scripts/demo_aws.sh encender` y, a los 3 minutos, `bash scripts/demo_aws.sh estado`: en la EC2-A deben estar los
   3 brokers y `streaming`; en la EC2-B, `postgres`, `airflow-scheduler`, `airflow-webserver` y `api`.
3. `bash scripts/demo_aws.sh preparar`: deja `main`, `NOTIFICAR_DESDE=lambda` (los correos los manda solo la Lambda)
   y el DAG `fraude_pipeline` activo.
4. `bash scripts/demo_aws.sh reservar 2000` y correr el guion completo con cronómetro.
5. `bash scripts/demo_aws.sh apagar`. Costo del ensayo: unos USD 0,20 por hora con las dos EC2.

Para ver Airflow en el navegador hace falta el Session Manager plugin del AWS CLI (instalador oficial de AWS) y
`bash scripts/demo_aws.sh airflow`. Sin él, `bash scripts/demo_aws.sh gold` muestra el estado del DAG en la terminal.

## El día de la sustentación
- **40 min antes:** `encender`, `estado` y `reservar 2000`. Después del ensayo los pagos ya quedaron cargados, así que
  **hay que reservarlos otra vez**; si no, en la demo no saldrán alertas nuevas.
- Abrir Power BI (`dashboards/powerbi/FraudLens.pbip`), el diagrama, el correo donde llegan las alertas y, si está el
  plugin, Airflow en http://localhost:8081.
- Tener abierta una terminal en la raíz del repo.

## Guion (8 minutos)
| Min | Qué se hace | Comando | Qué se dice |
|---|---|---|---|
| 0:00 | Estado del sistema | `estado` | «Dos EC2: la A con Kafka y Spark, la B con Airflow, PostgreSQL y la API. Todo en contenedores.» |
| 0:30 | **Productor → Kafka** | `enviar 2000` | «Mandamos los últimos 2.000 pagos reales del dataset, con 1 % de registros dañados. El sistema no conoce su etiqueta.» |
| 1:00 | **DAG de Airflow** | Airflow en el navegador, o `gold` | «`fraude_pipeline` corre cada 10 min: verifica que el streaming esté vivo, construye Gold, controla la calidad y registra en Glue. Si una tarea falla, las siguientes no corren y llega un correo.» |
| 2:30 | **Pregunta de negocio sobre Gold** | `consulta p6` | «P6 del E1: las 10 regiones con más monto sospechoso de la última semana, en 3 s y leyendo 5 MB.» |
| 3:30 | **Spark procesando** | `streaming` | «El micro-lote que tomó los 2.000 pagos: cuántos fueron válidos, cuántos a cuarentena y a la DLQ, y cuántas alertas nuevas. Tardó unos 3 minutos: esa es la latencia que no cumple la meta, y su causa es R4 leyendo la historia de cada cliente.» |
| 4:30 | Alertas | `alertas R4` | «Las alertas nuevas, en DynamoDB, escritas una sola vez.» Mostrar el correo resumen si ya llegó. |
| 5:30 | Acierto | `consulta p7` o Power BI, página «Acierto contra la realidad» | «Contra el fraude real: 20,9 % de precisión y 23,7 % de cobertura en la partición de prueba.» |
| 6:30 | Latencia por tramo | `consulta p5` | «La latencia partida en envío, cola y proceso.» |
| 7:15 | Cierre | | Dejar abierto el código de lo que el docente pida. Las rutas están en `docs/entregable2/guia_defensa.md`. |

**Idempotencia (si el docente la pide):** `repetir 500` reenvía 500 pagos que ya estaban. Unos 3 minutos después,
`streaming` muestra el lote con "0 nuevas": el MERGE no los vuelve a meter en Silver y DynamoDB no acepta una alerta
repetida, así que no sale otro correo.

## Si algo falla en vivo
La guía del curso dice que un fallo bien explicado puede mantener la nota. Causas probables y qué decir:
| Síntoma | Causa probable | Qué se hace |
|---|---|---|
| `ExpiredToken` o `AccessDenied` en el portátil | La sesión de `aws login` venció | `aws login --profile fraudlens` y repetir el comando |
| `estado` no muestra `postgres` en la EC2-B | PostgreSQL no arrancó con la máquina | Repetir `preparar`; si no, explicar que Airflow depende de PostgreSQL y se levanta con `docker compose up -d` |
| `streaming` no muestra el lote después de 4 min | El primer lote después de encender es más lento (~4 min) | Esperar un minuto más; explicar que es la latencia medida (P95 ~202 s) y su arreglo con una tabla de estado |
| Salen 0 alertas nuevas | No se corrió `reservar` antes de `enviar` | Es la idempotencia: los pagos ya estaban. Correr `reservar 2000` y volver a `enviar` |
| El DAG falla en `check_streaming_vivo` | El streaming no escribió en los últimos minutos | Es el control funcionando: el DAG no construye Gold con datos viejos. `streaming` muestra el estado |
| No llega el correo | La Lambda junta hasta 5 min antes de mandar | Mostrar las alertas en la API; el correo llega después |
| Sin internet en el salón | No se llega a AWS | Mostrar Power BI (modo Import, funciona sin conexión) y los tiempos del ensayo |

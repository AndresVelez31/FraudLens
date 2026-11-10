# Guía de defensa individual (E2)

La defensa individual vale el 40 % de la nota de la sustentación: **cualquiera de los cuatro puede recibir una pregunta
sobre cualquier parte**. Esta guía es para estudiar. Las cifras salen de la carga real de IEEE-CIS en AWS (us-east-2) y
están medidas sobre la partición de prueba (el 20 % final por tiempo), salvo que se diga otra cosa.

## Cifras que hay que saber de memoria
| Qué | Valor |
|---|---|
| Pagos cargados | 590.540 (182 días), 3,5 % fraude; 217.850 clientes (`card1 + addr1 + D1`) |
| Alertas | 28.154 en total, 0 repetidas; Silver con 590.540 filas exactas (0 duplicados tras 206 reenvíos) |
| Precisión / cobertura | 20,9 % / 23,7 % (reglas del E1 tal cual: 6,5 % / 3,3 %) |
| Falsos positivos | 32 por cada 1.000 pagos legítimos; 962 fraudes detectados |
| Precisión por regla | R4 22,2 %, R3 18,6 %, R2 13,4 %; R1 apagada |
| Por producto | C: 13,4 % de fraude real y detectamos el 40 %; W: 1,9 % y detectamos el 6,4 % |
| Latencia | P95 ~202 s (meta 30 s, **no se cumple**); mediana 109 s = cola ~45 s + proceso ~68 s + envío ~0 s |
| Costo | Carga completa ≈ USD 1,20 de EC2; cada consulta de Athena lee < 6 MB y tarda ~3 s |
| Modelo (AUC) | R4 con 17 variables del contrato 0,806; con las columnas de Vesta 0,908; 1.er lugar Kaggle 0,946 |

## Antes de la sustentación (checklist del equipo)
- [ ] Cada integrante vio el deck completo con las notas del orador y ensayó su parte (simulacro cronometrado, ≤ 15 min).
- [ ] Todos pueden explicar el diagrama `arquitectura.png` de izquierda a derecha, paso por paso (1 a 11).
- [ ] Power BI abre sin conexión (modo Import) y muestra la página «Acierto contra la realidad» en la partición de prueba.
- [ ] Si hay demo en vivo: encender las dos EC2 a mano (se apagan a las 23:00), revisar `docker compose ps` y avisar que
      cada alerta manda un correo (el resumen agrupa por lote).
- [ ] Se completó la última fila de `docs/bitacora_ia.md` (quién revisó qué). Sin bitácora: −10 %.
- [ ] Se verificó que `.env` y `data/` **no** están en el repositorio.

## Preguntas probables y respuesta corta

### Datos y medición
**¿Por qué IEEE-CIS y no datos colombianos?** No hay un dataset público de pagos colombianos con fraude etiquetado.
IEEE-CIS (Vesta, Kaggle) son pagos reales con su etiqueta. Lo único simulado es **cuándo llega** cada pago: el replayer
los manda en su orden real, acelerado ×1.300 (~50 por segundo).

**¿La etiqueta de fraude pasa por Kafka?** No. Se guarda aparte (`raw/ieee-cis/preparado/etiquetas.csv.gz`) y Gold la
cruza solo para medir (`es_fraude_real`, `particion_eval`). El sistema decide sin conocer la respuesta.

**¿Por qué no usan exactitud (accuracy)?** Porque diciendo «nada es fraude» se acierta el 96,5 %. Medimos precisión
(de cada 100 alertas cuántas eran fraude), cobertura (de cada 100 fraudes cuántos alertamos) y falsos positivos por
1.000 legítimas, con un presupuesto fijo de ~46 alertas por cada 1.000 pagos.

**¿Por qué partición por tiempo?** Se calibra y entrena con el 80 % más antiguo y se evalúa con el 20 % final, como
pasaría con pagos futuros. Una partición aleatoria dejaría ver el futuro y inflaría el resultado.

### Reglas y modelo
**¿Qué hace cada regla?** (`common/rules.py`, versión `ieee`)
* R1: ráfaga, 3 o más pagos del cliente en una ventana deslizante de 5 min. **Apagada** con `REGLAS_ACTIVAS=R2,R3,R4`:
  con datos reales casi no acertaba y daba la mayoría de los falsos positivos.
* R2: dispositivo nunca visto para el cliente y monto > 1,5× su promedio de 30 días.
* R3: comercio o dispositivo en la versión vigente de la lista de riesgo (calibrada con el 80 % de entrenamiento).
* R4: modelo LightGBM con 17 variables del evento y de la historia del cliente; alerta si el puntaje supera 0,155.

**¿Por qué LightGBM y no una red neuronal?** En datos tabulares los árboles con gradiente siguen ganando (Grinsztajn et
al., 2022) y el ganador de Kaggle también los usó. Además es rápido para puntuar en cada micro-lote.

**¿Por qué su AUC (0,806) es menor que el de Kaggle (0,946)?** Por datos, no por técnica. Con las columnas anonimizadas
de Vesta nuestro modelo llega a 0,908; en streaming solo usamos las 17 variables del contrato de datos. Un banco real sí
tendría esas columnas. El ganador identifica al cliente con `card1 + addr1 + D1`, la misma clave que usamos.

**¿Las variables del modelo son las mismas en entrenamiento y en streaming?** Sí: las calcula el mismo código
(`common/variables.py`) en los dos lados, así que no hay diferencias entre lo entrenado y lo que corre.

### Tiempo real y confiabilidad
**¿Cómo garantizan que no se pierde ni se duplica nada?** Kafka entrega al menos una vez; el «exactamente una vez» lo
ponemos en cada escritura:
* Bronze y cuarentena: escritura Delta con `txnAppId`/`txnVersion` (`common/tables.py`, `escribir_una_vez`); si el lote
  ya se escribió, Delta lo ignora.
* Silver: MERGE por `id_transaccion`, con reintentos si dos escrituras chocan (`con_reintentos`).
* DynamoDB: `PutItem` condicional (`attribute_not_exists(id_alerta)`); el `id_alerta` es determinista.
* Prueba real: el replayer se cayó tras 3 h y reenvió 206 pagos; Silver quedó con 590.540 exactos.

**¿Qué hacen con un mensaje roto?** JSON ilegible → topic `transacciones_dlq`, con el topic, partición y offset de
origen en los encabezados. JSON válido con campos inválidos → tabla de cuarentena con el motivo. Ninguno detiene el
pipeline.

**¿Cómo llegan los correos?** La alerta entra a DynamoDB; DynamoDB Streams despierta a la Lambda
`fraudlens-notificador`, que manda **un correo resumen por lote** por SNS (no uno por alerta, que inundaba el buzón).
Si la Lambda falla tras los reintentos, el lote queda en la cola SQS `fraudlens-notificador-fallos` para reprocesarlo.
La Lambda junta hasta 5 min (máximo de AWS), así que una ráfaga da un correo por shard del stream. Un resumen cada
30 min necesitaría un acumulador con EventBridge; en un banco real el analista ve una cola en una aplicación y el
correo es solo un aviso.

**¿Qué pasa si cae un broker?** Con 3 brokers y `min.insync.replicas=2` se sigue escribiendo. Con dos caídos, el
productor se detiene: prefiere no perder datos.

### Lakehouse y análisis
**¿Por qué Delta Lake?** Transacciones ACID sobre S3, MERGE, esquema obligatorio y 90 días de historial (viaje en el
tiempo).

**¿Por qué Gold procesa por fecha de llegada y no por fecha del pago?** Los pagos son de 2017-2018 reescalados, pero
llegan hoy. Si Gold filtrara por fecha del pago, ignoraría lo que acaba de entrar. Filtra por `ingest_ts` y la hora se
muestra en hora de Colombia (`ZONA_HORARIA`).

**¿Por qué Power BI en modo Import?** Athena cobra por dato leído. En Import se consulta una vez al actualizar y el
tablero se navega gratis y con las EC2 apagadas.

**¿Qué pasa si cambia el esquema de una tabla Gold?** `batch/publicar.py` compara las columnas con Glue y recrea la
tabla si cambiaron; antes Athena fallaba con `INVALID_GLUE_SCHEMA`.

**¿Dónde están los datos personales?** Solo en PostgreSQL. En el lake entran con HMAC-SHA-256 (clave en `.env`), no con
un hash simple, para que no se puedan revertir por diccionario.

### Arquitectura y costo
**¿Por qué Kafka en EC2 y no Kinesis o MSK?** No están en el plan gratuito del proyecto. Se migra a MSK cambiando solo
la dirección de los brokers.

**¿Cómo mantienen el costo bajo?** Regla del E1: servicio administrado solo si cobra por uso (S3, DynamoDB, Lambda,
SNS, SQS, Glue, Athena); lo que cobra por hora corre en contenedores en dos EC2 que se apagan solas a las 23:00 (COT).
La carga completa costó ≈ USD 1,20.

**¿Qué ofrece cada componente según CAP?** Kafka y S3: consistencia (CP). DynamoDB: lecturas eventualmente consistentes
(AP). Airflow/PostgreSQL: CP.

### Lo que no cumplimos (decirlo antes de que lo pregunten)
**¿Cumplen la latencia?** No. La meta era P95 < 30 s y medimos ~202 s. Lo medimos por tramos con las marcas de tiempo
de Bronze: el envío es instantáneo, el tiempo se va en procesar el lote (~68 s) y en la cola que eso genera (~45 s).
La causa: R4 relee desde Silver en S3 la historia del cliente en cada lote. El arreglo: una tabla de estado por cliente
(en memoria o DynamoDB) actualizada en cada lote.

**¿Qué más queda pendiente?** Retroalimentación del analista para reentrenar, probar TabPFN, una prueba de carga a
5.000 pagos por segundo, y que el replayer descargue el archivo antes de enviar (para no caerse en cargas largas).

## Lecciones de operar el sistema de verdad
* Un correo por alerta inunda el buzón → resumen por lote.
* Un servicio que arranca solo puede releer Kafka desde el principio → `STARTING_OFFSETS=latest`.
* Una carga de 3 h se cae → el replayer se reanuda con `--saltar`.
* Un esquema que cambia rompe Athena → Glue se actualiza solo.

## Reparto sugerido de qué domina cada uno (todos deben saber lo básico de todo)
| Integrante | Foco | Archivos |
|---|---|---|
| Andrés Felipe Vélez | Kafka, replayer, infraestructura AWS, Lambda notificador | `producer/`, `docker-compose.yml`, `infra/`, `lambdas/` |
| Samuel Samper | Streaming, reglas R1–R4, idempotencia, alertas | `streaming/`, `common/rules.py`, `common/r4.py`, `common/tables.py`, `common/alerts.py` |
| Sebastián Salazar | Gold, calidad, reproceso, Athena y Power BI, métricas de acierto | `batch/`, `dashboards/` |
| Hellen Yanes | Airflow, catálogos, PII/HMAC, API, modelo y evaluación, bitácora | `dags/`, `common/pii.py`, `api/`, `ml/`, `docs/evaluacion/` |

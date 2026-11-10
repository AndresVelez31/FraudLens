# Cambios frente al Entregable 1

El E2 conserva el núcleo del diseño del E1: Kafka, Spark Structured Streaming, Delta Lake en S3 con Bronze, Silver y
Gold, Airflow y DynamoDB. Lo que cambió responde a cuatro hechos nuevos:
- el profesor pidió datos reales y medir el acierto contra la realidad;
- la plataforma pasó de AWS Academy al plan gratuito de AWS;
- la prueba con tráfico real mostró problemas que el diseño no anticipaba;
- el profesor pidió Power BI.

Este documento lista cada cambio y su porqué, y cómo quedó cada requisito no funcional.

## 1. Cambios de diseño

| Área | Entregable 1 | Entregable 2 | Por qué |
|---|---|---|---|
| **Plataforma** | AWS Academy Learner Lab, us-east-1, USD 50, credenciales de 4 h | Proyecto propio en el plan gratuito de AWS, us-east-2, USD 100 de crédito y presupuesto de USD 40 con alertas | El Learner Lab apaga todo al cerrar la sesión y limita servicios; el plan gratuito permite operar de forma continua |
| **Cómputo** | 3 EC2 t3 (streaming, batch, consumo) | 2 EC2 m7i-flex.large (streaming; batch, API y PostgreSQL), apagado automático a las 23:00 con EventBridge Scheduler y operación por Systems Manager sin SSH | Metabase salió (ver Consumo) y la EC2-C quedó sin uso; el apagado automático evita pagar horas sin trabajo |
| **Datos** | Productor sintético con Faker y escenarios inventados | 590.540 transacciones reales de IEEE-CIS (Vesta) con la etiqueta de fraude; un replayer las reproduce en su orden y ritmo reales, acelerados ×1.300 | Con datos inventados no se puede medir el acierto. No hay un dataset público colombiano con fraude etiquetado |
| **Contrato del evento** | Datos personales de un cliente sintético | Se agregan `ts_emision`, `region` y `correo_dominio`; `id_cliente = card1 + addr1 + D1n`; el comercio es la línea de producto | Mapear IEEE-CIS al contrato sin perder información útil para las reglas y el modelo |
| **Reglas** | R1, R2 y R3 con umbrales fijados a mano (versión `v1`) | Versión `ieee` calibrada con el 80 % inicial; **R4 nueva**: LightGBM con 17 variables y umbral 0,155; **R1 apagada** (`REGLAS_ACTIVAS=R2,R3,R4`) | Las reglas del E1 acertaban 6,5 % de sus alertas. El modelo casi triplica la precisión y R1 era la mayor fuente de falsos positivos |
| **Evaluación** | No existía | Partición temporal 80/20, precisión, cobertura y falsos positivos por cada 1.000 pagos legítimos con presupuesto fijo de alertas; la etiqueta se cruza en Gold y el streaming nunca la ve | Requisito del profesor: medir el acierto contra la realidad sin hacer trampa con el futuro |
| **Notificación** | SNS publicado por Spark, un correo por alerta nueva | DynamoDB Streams → Lambda (lotes de hasta 5 min) → un correo resumen por SNS; los lotes que fallan quedan en SQS tras 2 reintentos | Una prueba mandó más de 500 correos. Además, Spark ya no espera a SNS ni pierde el resumen si se cae |
| **Gold** | Incremental por fecha del evento; perfil de R2 con el reloj del servidor; horas en UTC | Incremental por fecha de llegada (`ingest_ts`); perfil con el reloj de los datos; `dim_tiempo` en hora de Colombia; columnas de fraude real, región del pago y latencia por tramo | Con datos históricos que llegan hoy, el diseño del E1 dejaba casi todo fuera de Gold |
| **Consumo** | Metabase en EC2-C; preguntas P4 a P6 | Power BI en modo Import sobre Athena; P4 a P6 adaptadas a IEEE-CIS (región del pago, top 10 de regiones) y **P7 nueva**: acierto contra el fraude real | Power BI lo pidió el profesor; Import evita que Athena cobre en cada clic y funciona con las EC2 apagadas |
| **Confiabilidad** | MERGE en Silver y escritura condicional en DynamoDB | Además, escrituras idempotentes en Bronze y cuarentena (`txnAppId`/`txnVersion` de Delta), DLQ con su origen en los encabezados, MERGE de alertas por regla con reintento ante conflicto, y Glue que se actualiza si Gold cambia de columnas | Hallazgos 3 y 5 de la revisión de código y fallas vistas en la carga real |
| **Latencia** | Se medía contra la hora del evento | Se mide desde la emisión (`ts_emision`) y se divide en envío, espera en Kafka y procesamiento | Al reproducir un histórico, la hora del evento es de meses atrás |

## 2. Requisitos no funcionales: meta del E1 y resultado

| Requisito | Meta del E1 | Resultado medido en el E2 | Estado |
|---|---|---|---|
| RNF-01 Escalabilidad | 5.000 eventos/s sostenidos 30 min | Carga real de 590.540 pagos a ~50 eventos/s durante unas 3 h, con un retraso en Kafka de 1 a 2 min | No probado a 5.000/s |
| RNF-02 Disponibilidad (CAP) | Kafka CP con 3 brokers; DynamoDB AP | Configuración igual; con un broker caído, el productor no pierde mensajes (prueba local) | Cumplido en la configuración |
| RNF-03 Consistencia | Cero duplicados en Silver, Gold, DynamoDB y correos | 590.540 pagos exactos en Silver tras 206 reenvíos; 0 alertas repetidas; lotes repetidos sin duplicar (D1) | Cumplido |
| RNF-04 Latencia | Evento → alerta: 30 s en P95 | **202 s en P95** desde la emisión (cola 45 s, procesamiento 68 s), con R4 leyendo la historia del cliente desde Silver; consultas de Athena en unos 3 s y menos de 6 MB | **No cumplido** en carga; Athena cumplido |
| RNF-05 Tolerancia a fallos | Retomar desde el checkpoint en menos de 5 min | El replayer se cayó tras 3 h y se retomó sin perder ni duplicar; el streaming se reinició desde su checkpoint | Cumplido en la práctica |
| RNF-06 Trazabilidad | Offset de Kafka, `batch_id` y `rule_version` en Silver y Gold | Se conservan; Gold además guarda la latencia por tramo | Cumplido |
| RNF-07 Seguridad y privacidad | HMAC de datos personales, sin llaves en el repositorio | HMAC-SHA-256 en Silver; roles de IAM de mínimo privilegio por pieza; usuario de solo lectura para Power BI | Cumplido |
| RNF-08 Calidad de datos | Rechazo de más de 1 % detiene el pipeline | 0 transacciones reales en cuarentena; los registros dañados inyectados a propósito van a la DLQ o a cuarentena con su motivo | Cumplido |
| RNF-09 Costo | Menos de USD 50 | La carga completa costó unos USD 1,20 de EC2 (2 máquinas, 6 h); Athena, S3, DynamoDB y Lambda, centavos. El gasto total del semestre se confirma en AWS Billing | Dentro del presupuesto |

**Sobre la latencia (RNF-04):** el incumplimiento tiene una causa identificada. R4 lee de Silver toda la historia de los
clientes de cada lote, y Silver llegó a 590 mil filas, así que cada lote tardaba más de lo que llegaba. La corrección es
guardar el estado del cliente (conteos y promedios) en una tabla de baja latencia, como DynamoDB, actualizada en cada
lote. Es lo que hacen las arquitecturas de referencia con el estado de Flink.

## 3. Lo que no cambió

- **Kafka sigue siendo la ingesta**, con 3 brokers KRaft, 12 particiones y clave `id_cliente`. El profesor sugirió Kinesis, pero el plan gratuito no lo incluye.
- **El lakehouse** sigue igual: Delta Lake sobre S3 con Bronze, Silver y Gold, un esquema en estrella en Gold y Glue con Athena para consultarlo.
- **Airflow** sigue con los mismos dos DAG: `catalogos_diario` y `fraude_pipeline`.
- **DynamoDB** sigue como almacén de alertas, con escritura condicional.
- **La regla de costo del E1:** un servicio administrado solo se usa si cobra por uso.

Detalle de cada cambio en los PR #1 a #23 del repositorio y en `docs/evaluacion/`.

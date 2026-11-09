# Guía de defensa individual (E2)

La defensa individual vale el 40 % de la nota de la sustentación: **cualquiera de los cuatro puede recibir una pregunta
sobre cualquier parte**. Esta guía es para estudiar; no sustituye correr el sistema al menos una vez cada uno.

## Antes de la sustentación (checklist del equipo)
- [ ] Cada integrante corrió `docker compose up -d --build` en su máquina y siguió la demo del README.
- [ ] Se corrigió lo que falló en Docker (el código se probó sin Docker; ver README, «Qué se probó y qué no»).
- [ ] Se midió la latencia real con `python -m batch.consultas p5` y se actualizó el README y la slide 11.
- [ ] Se completó la última fila de `docs/bitacora_ia.md` (quién revisó qué). Sin bitácora: −10 %.
- [ ] Se abrió el PR final en GitHub con todo esto (rama → `main`).
- [ ] Se verificó que `.env` **no** está en el repositorio.
- [ ] Las cédulas del documento E1 son las de la entrega; confirmar si la rúbrica pide códigos estudiantiles EAFIT.

## Preguntas probables y respuesta corta

**¿Cómo garantizan que no se pierde ni se duplica una transacción (exactly-once)?**
Kafka: `acks=all`, productor idempotente, `min.insync.replicas=2`. Spark: checkpoint + `foreachBatch` con MERGE por
`id_transaccion`+`fecha`, así que reprocesar un lote no inserta de nuevo. Es *at-least-once* en la entrega con efecto
*exactly-once* en el resultado (`common/lake.py`, `streaming/job.py`).

**¿Qué pasa si el job muere a mitad de un lote?** Al reiniciar retoma del checkpoint y reprocesa el lote; el MERGE y el
`id_alerta` determinista (sha2 de regla+cliente+ventana/transacción+versión) evitan duplicados. Probado con `kill -9`.

**¿Por qué la alerta no se duplica en DynamoDB?** PutItem condicional (`attribute_not_exists(id_alerta)`); solo las
alertas realmente nuevas disparan SNS. Costo: si muere entre PutItem y SNS, esa notificación se pierde (at-most-once).

**¿Cómo funciona R1?** Ventana deslizante de 5 min con paso de 1 min por cliente, con watermark; modo *update* y
alerta si hay más de 5 transacciones o más de $2.000.000. Una ráfaga genera varias alertas por ventanas que se solapan.

**¿Cómo funciona R2?** Compara contra `perfil_cliente` (promedio de 30 días, calculado en Gold) y la dimensión de
dispositivos: dispositivo nunca visto y monto > 3× el promedio.

**¿Qué hacen con un mensaje roto?** JSON ilegible → topic `transacciones_dlq`. JSON válido con campos inválidos →
tabla de cuarentena con el motivo. Ninguno detiene el pipeline. El control de calidad falla si el rechazo supera 1 %.

**¿Dónde están los datos personales?** Solo en PostgreSQL. En el lake entran con HMAC-SHA-256 (clave secreta en `.env`),
no con un hash simple, para que no se pueda revertir por diccionario (RNF-07).

**¿Por qué Delta Lake?** Transacciones ACID sobre S3, MERGE, viaje en el tiempo (90 días) y esquema obligatorio.

**¿Por qué estrella en Gold?** Las preguntas P4–P6 son agregaciones sobre hechos por dimensiones; `dim_cliente` es SCD2
para conservar la ciudad/segmento que tenía el cliente cuando ocurrió la transacción.

**¿Qué es el reproceso (RF-16)?** `batch.reproceso` recalcula R1–R3 sobre Silver con otra versión de reglas y escribe en
Gold con su `rule_version`; no toca Bronze/Silver ni genera notificaciones. Permite comparar v1 vs v2.

**¿Qué ofrece cada componente según CAP?** Kafka y S3: consistencia (CP). DynamoDB: lecturas eventualmente consistentes
(AP) — la API lo advierte. Airflow/Postgres: CP.

**¿Qué pasa si cae un broker?** Con 3 brokers y `min.insync.replicas=2` se sigue escribiendo. Con dos caídos, el productor
se detiene (prefiere no perder datos).

**¿Cumplen la latencia?** Meta: alerta en < 2 min. Medido en 2 núcleos: R1 ~11 s, R2/R3 hasta ~65 s (incluye lotes acumulados tras reinicio); el P95 < 30 s del RNF no está garantizado ahí. Decir el valor medido en
AWS, sin redondear a favor.

**¿Qué NO está resuelto?** Metabase localmente (no lee Delta; en AWS va por Athena), notificación exactly-once, y falta
la medición en el Learner Lab. Decirlo es mejor que que lo descubra el jurado.

## Reparto sugerido de qué domina cada uno (todos deben saber lo básico de todo)
| Integrante | Foco | Archivos |
|---|---|---|
| Andrés Felipe Vélez | Kafka, productor, infraestructura/compose | `producer/`, `docker-compose.yml`, `infra/` |
| Samuel Samper | Streaming, reglas R1–R3, alertas | `streaming/`, `common/rules.py`, `common/alerts.py` |
| Sebastián Salazar | Gold, calidad, reproceso, consultas P4–P6 | `batch/`, `dashboards/athena/` |
| Hellen Yanes | Airflow, catálogos, PII/HMAC, API, bitácora | `dags/`, `batch/catalogos.py`, `common/pii.py`, `api/` |

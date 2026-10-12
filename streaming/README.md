# streaming

**Responsable:** Samuel Samper Cardona · **Par revisor:** Sebastián Salazar Henao

Job de Spark Structured Streaming (trigger 5 s): Kafka → Bronze → Silver (dedup con watermark, DLQ, cuarentena, MERGE),
reglas R1–R3, alertas idempotentes en DynamoDB y Silver, notificación por SNS, checkpoints en S3 y latido del job.

Requisitos: RF-04 a RF-09, RNF-03, RNF-04, RNF-08.

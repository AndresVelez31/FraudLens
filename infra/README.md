# infra

**Responsable:** Andrés Felipe Vélez Álvarez · **Par revisor:** Samuel Samper Cardona

- Recursos de AWS: bucket S3 `fraudlens-lake` (prefijos `landing/`, `bronze/`, `silver/`, `gold/`, `checkpoints/`),
  tabla DynamoDB `alertas_fraude` (on-demand), topic SNS `alertas-fraude`, base de datos de Glue.
- EC2-A (t3.large, perfil `streaming`), EC2-B (t3.large, perfil `batch`), EC2-C (t3.medium, perfil `consumo`) con `LabInstanceProfile`.
- Imágenes Docker propias con los JAR de Spark, Delta, `hadoop-aws` y el conector de Kafka.

Requisitos: RF-01, RNF-01, RNF-02 (Kafka), RNF-05, RNF-09.

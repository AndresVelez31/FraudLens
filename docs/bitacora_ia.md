# Bitácora de delegación a agentes de IA

Obligatoria según la guía del proyecto (sin bitácora se descuenta el 10 % del E2) y la política de IA del curso.
Cada integrante agrega una fila por tarea en la que use un agente: qué delegó y qué decidió el equipo.

| Entregable | Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|---|
| E1 | Definición del problema y preguntas P1–P6 (Sección 1) | Parcial | Redactada por el equipo. El agente (Claude) buscó cifras de mercado con su fuente (Banco de la República, Asobancaria, Nequi) y las incorporó. |
| E1 | Requisitos funcionales y no funcionales (Sección 2) | Parcial | Redactados por el equipo. El agente señaló inconsistencias (RF-04 frente a RNF-03, CAP por componente, RF faltante para la API) y los adaptó al despliegue en AWS. |
| E1 | Elección de tecnologías (Kafka, Spark, Delta Lake, Airflow) | No | Decisión de diseño central tomada por el equipo antes de usar el agente. |
| E1 | Despliegue en AWS Learner Lab (contenedores en EC2 y servicios por uso) y cambio de MongoDB a DynamoDB | Parcial | Propuestos por el agente tras revisar los laboratorios del curso y el límite de instancias del Learner Lab; el equipo los aprobó. |
| E1 | Redacción de los ADR (Sección 4) | Parcial | El equipo definió contexto, alternativas y decisión; el agente ayudó a redactar y a citar los conceptos del curso. |
| E1 | Plan de implementación (Sección 5) | Parcial | Reparto de bloques acordado por el equipo; el agente redactó el cronograma y la tabla de riesgos. |
| E1 | Dibujo del diagrama en draw.io (notación AWS) y exportación | Sí | El agente generó el .drawio con los íconos oficiales de AWS a partir del diseño del equipo. Bajo valor pedagógico. |
| E1 | Maquetación en LaTeX, referencias APA y anexos | Sí | Tarea de formato; bajo valor pedagógico. |
| E2 | Diseño de la implementación (reglas, idempotencia, DLQ/cuarentena, Gold en estrella) a partir del E1 | Parcial | El agente (Claude) propuso la estructura del código y las decisiones técnicas listadas en el README; el equipo debe poder explicar y defender cada una (ver `docs/entregable2/guia_defensa.md`). |
| E2 | Código del productor, streaming (Kafka → Bronze/Silver, R1–R3, alertas), batch (catálogos, Gold, calidad, reproceso), DAG de Airflow y API | Sí | Escrito por el agente a partir de los requisitos del E1. Alto valor pedagógico: cada integrante debe revisarlo y entender al menos su bloque antes de la sustentación. |
| E2 | docker-compose, Dockerfiles, `init.sql` y `.env.example` | Sí | Generados por el agente. Validados solo con `docker compose config`; el equipo debe levantarlos y corregir lo que falle. |
| E2 | Pruebas (pytest y prueba end-to-end sin Docker con Kafka local, Spark y DynamoDB simulado) | Sí | El agente las escribió y las ejecutó; los resultados y límites están en el README (sección «Qué se probó y qué no»). |
| E2 | README, esta bitácora, slides y guía de defensa | Sí | Redactados por el agente; el equipo debe revisarlos y ajustarlos a lo que realmente ejecute en la demo. |
| E2 | Revisión del código, ejecución de la demo en el Learner Lab, medición de latencia y apertura del PR | No | Pendiente del equipo. Completar aquí quién revisó qué y qué se cambió. |

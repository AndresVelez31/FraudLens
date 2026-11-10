# Estado del arte: detección de fraude en pagos en tiempo real

FraudLens combina lo que hace la industria (reglas explicables y un modelo de árboles con gradiente sobre un flujo de
eventos) con una evaluación honesta contra fraude confirmado. Con las 17 variables que el streaming conoce en tiempo
real alcanza 20,9 % de precisión y 23,7 % de cobertura en la partición de prueba de IEEE-CIS, unas tres veces la
precisión de sus reglas originales. El techo de ese mismo dataset, con toda la información de Vesta y validación
cuidadosa, está en AUC 0,946 (primer lugar de Kaggle); nuestro modelo "contrato" llega a AUC 0,806 y el que usa las
431 columnas de Vesta, a 0,908. La distancia se explica más por la información disponible en el evento que por el
algoritmo.

## 1. Enfoques y dónde queda FraudLens

| Enfoque | Qué dice la literatura | Qué hace FraudLens | Resultado en FraudLens |
|---|---|---|---|
| **Reglas de velocidad, listas y umbrales** | Es la base de los sistemas comerciales de monitoreo: son explicables y fáciles de auditar, pero generan muchos falsos positivos y hay que recalibrarlas a mano. | R1 (ráfaga), R2 (dispositivo nuevo y monto inusual) y R3 (lista de riesgo), calibradas con el 80 % inicial de los datos. | Precisión 7,4 % y cobertura 10,0 % con 46 alertas por cada 1.000 pagos. R1 se apagó porque casi no acertaba. |
| **Aprendizaje supervisado con clases desbalanceadas y etiquetas tardías** | Dal Pozzolo et al. (2018) muestran que el fraude real llega como flujo, con desbalance extremo, deriva de conceptos y etiquetas que solo se conocen días después (contracargos), y que la evaluación debe respetar ese orden. | Partición temporal 80/20; métricas de precisión, cobertura y falsos positivos por 1.000 pagos legítimos con un presupuesto fijo de alertas; la etiqueta se cruza en Gold y el streaming nunca la ve. | La cifra honesta es la de prueba (20,9 %). Con los datos de entrenamiento el mismo modelo aparenta 39,1 %: el sesgo que Dal Pozzolo advierte. |
| **Árboles con gradiente en datos tabulares** | Grinsztajn et al. (2022) muestran que los árboles (XGBoost, LightGBM) siguen superando al aprendizaje profundo en datos tabulares de tamaño medio. LightGBM (Ke et al., 2017) los entrena rápido con GOSS y EFB. | R4: LightGBM con 17 variables calculadas por el mismo módulo en entrenamiento y en streaming (paridad verificada: 544 alertas idénticas). | AUC 0,806. A igual número de alertas, casi triplica la precisión de las reglas (20,9 % frente a 7,4 %). |
| **El estado del arte en IEEE-CIS** | El primer lugar de la competencia (McDonald y Deotte, 2021) obtuvo AUC 0,9459 en el leaderboard privado. Su idea clave fue reconstruir al cliente combinando `card1`, `addr1` y `D1`, y agregar su comportamiento; usó un ensamble de XGBoost, CatBoost y LightGBM con validación por meses. | `id_cliente` se construye exactamente con esa llave (`card1 + addr1 + D1n`). El modelo "vesta" con las 431 columnas originales sirve de techo de referencia. | Vesta: AUC 0,908 y precisión 43,0 %. No es usable en tiempo real: esas columnas no llegan en el evento. |
| **Modelos base tabulares (IA preentrenada)** | TabPFN (Hollmann et al., 2025, *Nature*) es un modelo preentrenado con tablas sintéticas que clasifica por aprendizaje en contexto, sin entrenar por dataset; supera a líneas base afinadas en tablas de hasta ~10.000 filas. | Pendiente como extra (tarea F4): usar una muestra balanceada del 80 % como contexto y evaluar en el 20 % final contra LightGBM y las reglas. | No medido todavía. |
| **Sesgo y deriva en datasets de fraude** | Jesus et al. (2022) publican Bank Account Fraud, un banco de pruebas tabular con desbalance, dinámica temporal y sesgo inyectado, para medir rendimiento y equidad a la vez. | Se mide por partición temporal y por línea de producto (P7). No se mide equidad: IEEE-CIS no trae atributos protegidos. | Hallazgo: en el producto W (79 % de los pagos) la cobertura es 6 %; en C, 40 %. |

## 2. Arquitecturas de referencia en AWS

AWS publica varios patrones de detección de fraude en tiempo real. Todos tienen la misma forma que FraudLens: ingesta
de eventos, procesamiento en flujo con un modelo, un almacén de alertas, notificación y analítica histórica.

| Patrón de AWS | Ingesta | Procesamiento | Modelo | Alertas y analítica | Equivalente en FraudLens |
|---|---|---|---|---|---|
| Fraud prevention con Amazon MSK y Amazon Fraud Detector (whitepaper de streaming moderno) | Amazon MSK | Managed Service for Apache Flink | Amazon Fraud Detector (`GetEventPrediction`) | Lambda → SNS; OpenSearch | Kafka en EC2 → Spark Structured Streaming → LightGBM propio → DynamoDB → Lambda → SNS; Athena y Power BI |
| Guidance for Transactional Fraud Detection on AWS | Kinesis Data Streams | Flink y Lambda | SageMaker y Neptune (grafos) | Timestream, Athena, QuickSight | Mismo patrón de capas; FraudLens no usa grafos |
| Guidance for Payments Fraud Prevention on AWS | Streaming de los datos de pago | Evaluación casi en tiempo real | Según el proveedor de pagos | Revisión de casos | Mismo objetivo, con servicios gratuitos o autogestionados |

**Por qué FraudLens no usa los servicios de esos patrones:** el proyecto corre en el plan gratuito de AWS, donde
Kinesis, MSK y Firehose están bloqueados, y MWAA o EMR cobran por hora encendidos. La regla de diseño, heredada del E1,
es usar un servicio administrado solo si cobra por uso (S3, DynamoDB, Lambda, SNS, SQS, Glue, Athena) y correr en
contenedores lo que cobra por hora. El diseño se migra a los servicios administrados sin cambiar la lógica: Kafka a MSK
cambiando la dirección de los brokers, Spark a EMR o Glue Streaming, Airflow a MWAA.

## 3. Lo que FraudLens aporta y lo que le falta

**Aporta:**
- **Evaluación contra la realidad dentro del producto:** la precisión y la cobertura están en el tablero (P7), no solo en un notebook.
- **Paridad verificada** entre entrenamiento y producción, que la literatura señala como causa frecuente de modelos que fallan al desplegarse.
- **Escrituras idempotentes de punta a punta** (Delta MERGE, transacciones idempotentes de Delta, escritura condicional en DynamoDB), sobre la base de Delta Lake (Armbrust et al., 2020).

**Le falta, frente al estado del arte:**
- **Variables de cliente más ricas:** el primer lugar de Kaggle agregó cientos de estadísticas por cliente; FraudLens usa 17 que se pueden calcular en milisegundos. Ahí está la mayor parte de la distancia entre AUC 0,806 y 0,946.
- **Retroalimentación de los analistas:** Dal Pozzolo et al. (2018) proponen entrenar con lo que confirman los investigadores además de las etiquetas tardías. FraudLens no captura todavía esa confirmación.
- **Estado en memoria rápida:** R4 lee la historia del cliente desde Silver en cada lote, lo que explica la mayor parte de la latencia (P95 de 202 s bajo carga). Los patrones de AWS guardan ese estado en el motor de flujo (Flink) o en una tabla de baja latencia.
- **Grafos y equidad:** no se modelan relaciones entre cuentas ni se mide sesgo.

## Referencias

- Amazon Web Services. (s. f.). *Guidance for Transactional Fraud Detection on AWS*. https://aws.amazon.com/solutions/guidance/transactional-fraud-detection-on-aws
- Amazon Web Services. (s. f.). *Guidance for Payments Fraud Prevention on AWS*. https://aws.amazon.com/solutions/guidance/payments-fraud-prevention-on-aws
- Amazon Web Services. (s. f.). *Streaming architecture patterns using a modern data architecture* (Build Modern Data Streaming Architectures on AWS). https://docs.aws.amazon.com/whitepapers/latest/build-modern-data-streaming-analytics-architectures/streaming-analytics-architecture-patterns-using-a-modern-data-architecture.html
- Armbrust, M., Zaharia, M. et al. (2020). Delta Lake: High-performance ACID table storage over cloud object stores. *Proceedings of the VLDB Endowment, 13*(12), 3411–3424. https://vldb.org/pvldb/vol13/p3411-armbrust.pdf
- Dal Pozzolo, A., Boracchi, G., Caelen, O., Alippi, C. y Bontempi, G. (2018). Credit card fraud detection: A realistic modeling and a novel learning strategy. *IEEE Transactions on Neural Networks and Learning Systems, 29*(8). https://doi.org/10.1109/TNNLS.2017.2736643
- Grinsztajn, L., Oyallon, E. y Varoquaux, G. (2022). Why do tree-based models still outperform deep learning on typical tabular data? En *Advances in Neural Information Processing Systems 35* (Datasets and Benchmarks). https://arxiv.org/abs/2207.08815
- Hollmann, N., Müller, S., Purucker, L., Krishnakumar, A., Körfer, M., Hoo, S. B., Schirrmeister, R. T. y Hutter, F. (2025). Accurate predictions on small data with a tabular foundation model. *Nature, 637*(8045), 319–326. https://doi.org/10.1038/s41586-024-08328-6
- Jesus, S. et al. (2022). Turning the tables: Biased, imbalanced, dynamic tabular datasets for ML evaluation. En *Advances in Neural Information Processing Systems 35* (Datasets and Benchmarks). https://arxiv.org/abs/2211.13358
- Ke, G., Meng, Q., Finley, T., Wang, T., Chen, W., Ma, W., Ye, Q. y Liu, T.-Y. (2017). LightGBM: A highly efficient gradient boosting decision tree. En *Advances in Neural Information Processing Systems 30*, 3146–3154. https://proceedings.neurips.cc/paper/2017/file/6449f44a102fde848669bdd9eb6b76fa-Paper.pdf
- McDonald, C. y Deotte, C. (26 de enero de 2021). *Leveraging machine learning to detect fraud: Tips to developing a winning Kaggle solution*. NVIDIA Developer Blog. https://developer.nvidia.com/blog/leveraging-machine-learning-to-detect-fraud-tips-to-developing-a-winning-kaggle-solution/

Las cifras de FraudLens salen de `docs/evaluacion/` (F2 y F3) y de Athena sobre Gold después de la carga del 9 y 10 de
noviembre de 2026.

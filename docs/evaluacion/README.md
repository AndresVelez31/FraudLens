# Evaluación de FraudLens contra la realidad (tareas D2, F1, F2, F3 y D5)

**Pregunta del cliente:** ¿cuántas de nuestras alertas son fraude de verdad y cuántos fraudes detectamos?

Se responde con transacciones reales de IEEE-CIS (Vesta) que traen la etiqueta de fraude real (`isFraud`). Se usan
**las mismas funciones de reglas del sistema** (`common/rules.py`), no una copia. Para no hacer trampa:

- Los umbrales se calibran **solo con el 80 % inicial en el tiempo** (entrenamiento, 472.432 transacciones).
- Se evalúan **solo con el 20 % final** (prueba, 118.108 transacciones, tasa base de fraude 3,44 %), con la historia
  anterior disponible para R1 y R2, como pasaría en producción.
- Una transacción cuenta como alertada si tiene alerta propia (R2, R3) o si cae en una ventana de R1 que alertó, igual
  que en la tabla Gold `fact_transacciones.es_sospechosa`.

Se reproduce con:

```bash
python -m batch.evaluacion calibrar --datos data/ieee-cis/preparado   # -> calibracion.json
python -m batch.evaluacion evaluar  --datos data/ieee-cis/preparado   # -> resultados.json y resultados.md
```

## Calibración (D2, solo entrenamiento)

Criterio: máximo F1 con a lo sumo **50 alertas por cada 1.000 transacciones** (el equipo de análisis no puede revisar
más del 5 % de los pagos). Detalle de todas las combinaciones en [`calibracion.json`](calibracion.json).

| Regla | v1 (E1) | `ieee` (calibrada) | Por qué |
|---|---|---|---|
| R1 ráfaga | más de 5 transacciones o más de COP 2.000.000 en 5 min | **3 o más transacciones en 5 min**, sin umbral de monto | Con datos reales, 3 o más ya duplica la tasa de fraude; el monto no mejoró ninguna combinación |
| R2 dispositivo nuevo | monto > 3× el promedio de 30 días | monto > **1,5×** el promedio | Con 3× casi nunca dispara |
| R3 lista de riesgo | — | 43 dispositivos (lista v100) | Aprendida solo con entrenamiento |

## Resultados en prueba (F1)

| Versión | Regla | Alertadas | Precisión | Cobertura | Falsos positivos | Alertas/1.000 | Monto de fraude detectado |
|---|---|---|---|---|---|---|---|
| v1 | R1 | 1.294 | 0,0 % | 0,0 % | 1.294 | 11,0 | 0,0 % |
| v1 | R2 | 262 | 14,5 % | 0,9 % | 224 | 2,2 | 0,9 % |
| v1 | R3 | 533 | 18,6 % | 2,4 % | 434 | 4,5 | 1,1 % |
| **v1** | **Cualquiera** | **2.076** | **6,5 %** | **3,3 %** | **1.941** | **17,6** | **1,9 %** |
| ieee | R1 | 4.303 | 5,3 % | 5,6 % | 4.075 | 36,4 | 4,1 % |
| ieee | R2 | 749 | 13,5 % | 2,5 % | 648 | 6,3 | 1,9 % |
| ieee | R3 | 533 | 18,6 % | 2,4 % | 434 | 4,5 | 1,1 % |
| **ieee** | **Cualquiera** | **5.479** | **7,4 %** | **10,0 %** | **5.071** | **46,4** | **6,7 %** |

Tabla generada: [`resultados.md`](resultados.md). Cifras completas, con la matriz de confusión, en
[`resultados.json`](resultados.json).

## Cómo leerlo (en lenguaje de cliente)

- **Con las reglas calibradas, de cada 100 alertas, 7 son fraude real**, y se detecta **1 de cada 10 fraudes**.
  Al azar, de cada 100 pagos revisados solo 3,4 serían fraude: las reglas concentran el riesgo unas **2,2 veces**.
- **La recalibración triplica la detección:** la cobertura pasa de 3,3 % a 10,0 % y el monto de fraude detectado de
  1,9 % a 6,7 %, dentro del límite de 50 alertas por cada 1.000 pagos.
- **La regla más precisa es la lista de riesgo (R3):** 18,6 %, unas 5,4 veces la tasa base, aunque cubre poco.
- **Costo:** se marca el 4,6 % de los pagos y el 92,6 % de esas alertas son falsos positivos. Cada uno es un cliente
  legítimo al que habría que verificar.

## Por qué la aproximación funciona solo en parte

1. **Los datos tienen poca historia por cliente:** la mediana es de 1 transacción por cliente, así que las reglas de
   comportamiento (ráfaga y dispositivo nuevo) tienen poco con qué compararse.
2. **R2 casi no discrimina en estos datos:** el fraude es 7,7 % con dispositivo nuevo y 7,2 % sin él, y solo el 20 %
   de las transacciones trae dispositivo.
3. **R1 se confunde con cuentas legítimas de alto volumen:** en prueba, un solo "cliente" (`U15775_330_129`) tiene
   1.412 transacciones, ráfagas de hasta 35 en 5 minutos y **ningún fraude**. Produce casi todos los falsos positivos
   de R1 con los umbrales del E1. Los sistemas reales lo resuelven con una **lista blanca** de cuentas de alto volumen
   verificadas; queda como mejora.
4. **Las señales más fuertes no están en las reglas:** el tipo de producto (C: 11,7 % de fraude) y el dominio de
   correo (`mail.com`: 19 %) separan mejor que la velocidad o el dispositivo.

## Reglas contra modelo (F2)

Modelo de referencia **LightGBM** (`ml/modelo_lightgbm.py`), entrenado con la **misma partición temporal** y evaluado en
las mismas 118.108 transacciones de prueba. Dos versiones:

- **Modelo "contrato"** (17 variables): solo lo que FraudLens puede calcular en el streaming con el evento y la
  historia previa del cliente, sin mirar el futuro. Entre otras: monto, tipo de producto, región, dominio de correo,
  pagos del cliente en 5 min, 1 h y 24 h, promedio de 30 días y uso previo del dispositivo. Es el candidato a regla R4.
- **Modelo "vesta"** (431 variables): todas las columnas originales de IEEE-CIS, que el evento de FraudLens no trae.
  Muestra hasta dónde se puede llegar con toda la información.

**Con el mismo presupuesto de alertas que las reglas (46,4 por cada 1.000 pagos):**

| Enfoque | Precisión | Cobertura | Monto de fraude detectado | AUC-ROC | AUC-PR |
|---|---|---|---|---|---|
| Reglas del E1 (v1)* | 6,5 % | 3,3 % | 1,9 % | — | — |
| Reglas calibradas (`ieee`) | 7,4 % | 10,0 % | 6,7 % | — | — |
| **LightGBM "contrato"** | **20,9 %** | **28,2 %** | **27,1 %** | 0,806 | 0,160 |
| LightGBM "vesta" (referencia) | 43,0 % | 58,0 % | 55,3 % | 0,908 | 0,549 |

\* Con v1 se generan solo 17,6 alertas por cada 1.000 pagos.

**Con la misma cobertura que las reglas (detectar el 10 % del fraude):**

| Enfoque | Precisión | Alertas por cada 1.000 pagos |
|---|---|---|
| Reglas calibradas | 7,4 % | 46,4 |
| LightGBM "contrato" | 28,1 % | 12,3 |
| LightGBM "vesta" | 97,2 % | 3,6 |

**Lectura:**
- Con la misma carga de trabajo para los analistas, el **modelo con las variables que ya tiene FraudLens acierta
  2,8 veces más que las reglas** (21 de cada 100 alertas contra 7) y **detecta 2,8 veces más fraude** (28 % contra
  10 %).
- Para detectar el mismo 10 % de fraude, el modelo necesita **casi 4 veces menos alertas** (12 contra 46 por cada
  1.000 pagos). Son casi 4 veces menos clientes legítimos molestados.
- Las variables más útiles del modelo "contrato" son el tipo de producto, la región, el monto, el uso previo del
  dispositivo y el dominio de correo. Son justamente las señales que las reglas no usan.
- El modelo "vesta" muestra que, con más información del pago, se duplica otra vez la precisión. Su AUC-ROC de 0,908
  está por debajo del ~0,95 de las mejores soluciones de la competencia. Esas soluciones usan mucha ingeniería de
  variables y validación aleatoria, que es más fácil que nuestra partición en el tiempo.

**Conclusión para la defensa:** las reglas son explicables, rápidas y fáciles de auditar, pero con estos datos su
precisión es baja. El camino para mejorar es un **modelo (R4) que use las señales del evento**, puntuando cada pago
en el streaming. Las reglas quedan como explicación y como respaldo. Los modelos entrenados se guardan en
`data/modelos/`, fuera del repositorio.

## Comparación final (F3)

¿Conviene reemplazar las reglas, combinarlas con el modelo o usar el modelo solo para filtrarlas? Se probaron todas
las opciones sobre las mismas transacciones de prueba (`ml/comparacion_final.py`). Las fracciones del filtro son
fijas (75, 50 y 25 %), así que no se ajustaron mirando las etiquetas de prueba. Tabla completa en
[`comparacion_final.md`](comparacion_final.md).

| Escenario | Alertas por 1.000 | Precisión | Cobertura | Falsos positivos por 1.000 legítimas | Monto de fraude detectado |
|---|---|---|---|---|---|
| A. Reglas calibradas (hoy) | 46,4 | 7,4 % | 10,0 % | 44,5 | 6,7 % |
| B. Modelo "contrato" solo | 46,4 | 20,9 % | 28,2 % | 38,0 | 27,1 % |
| **C. R2 y R3 + modelo en lugar de R1** | **46,4** | **19,8 %** | **26,6 %** | **38,6** | **25,7 %** |
| D. Mitad de las reglas con mayor puntaje + modelo | 46,4 | 17,7 % | 23,8 % | 39,5 | 24,0 % |
| E. Reglas filtradas, se conserva el 50 % | 23,2 | 12,1 % | 8,2 % | 21,1 | 5,0 % |
| F. Reglas filtradas, se conserva el 25 % | 11,6 | 18,5 % | 6,2 % | 9,8 | 4,1 % |
| G. Modelo "vesta" (referencia) | 46,4 | 43,0 % | 58,0 % | 27,4 | 55,3 % |
| **H. Lo que corre en el streaming (D5): R2, R3 y R4 con umbral fijo** | **42,5** | **19,9 %** | **24,5 %** | **35,3** | **24,4 %** |

Al filtrar cada regla y quedarse con la mitad de sus alertas de mayor puntaje, la precisión sube así:

| Regla | Sola | Mitad con mayor puntaje | Aciertos que conserva |
|---|---|---|---|
| R1 ráfaga | 5,3 % | 7,8 % | 73 % |
| R2 dispositivo nuevo | 13,5 % | 21,9 % | 81 % |
| R3 lista de riesgo | 18,6 % | 29,3 % | 79 % |

**Lectura:**
- **El modelo agrega lo que las reglas no ven.** Con la misma carga de trabajo, cualquier escenario con modelo (B, C
  y D) detecta entre 2,4 y 2,8 veces más fraude que las reglas solas.
- **R1 es la regla débil.** Aun filtrada, apenas pasa de 5,3 % a 7,8 % de precisión. Si se cambia R1 por el modelo y
  se mantienen R2 y R3 (escenario C), se obtiene casi lo mismo que con el modelo solo (19,8 % contra 20,9 %) y se
  conservan dos reglas fáciles de explicar al cliente.
- **Si el equipo de análisis tiene poca capacidad, el modelo sirve como filtro.** Quedarse con el 25 % de las alertas
  de reglas con mayor puntaje (F) reduce los falsos positivos 4,5 veces (de 44,5 a 9,8 por cada 1.000 clientes
  legítimos) y conserva más de la mitad de los aciertos.
- **Techo:** con toda la información de Vesta (G) se duplicaría otra vez la detección. Para acercarse a ese techo
  habría que enriquecer el evento, por ejemplo con más historia de la tarjeta y datos de identidad del dispositivo.

**Recomendación al cliente (escenario C):** en el streaming,
- mantener **R2** y **R3**, que son precisas y explicables;
- reemplazar **R1** por el modelo **R4** ("contrato"), con un umbral que respete el presupuesto de 50 alertas por cada
  1.000 pagos;
- usar el puntaje del modelo para **ordenar la cola** del analista, con lo más riesgoso primero.

Así la precisión pasa de 7 % a cerca de 20 % y la detección de 10 % a cerca de 25 % del fraude, sin más alertas. Es
lo que ya corre en el streaming (escenario H): ver "R4 en el streaming (D5)".

Se reproduce con:

```bash
python -m batch.evaluacion exportar --datos data/ieee-cis/preparado          # marcas de las reglas (imagen fraudlens-spark)
python ml/modelo_lightgbm.py                                                 # modelos, puntajes y umbral de R4 (imagen fraudlens-ml)
python ml/comparacion_final.py                                               # -> comparacion_final.json y .md
```

## R4 en el streaming (D5)

El modelo "contrato" ya corre en el streaming como **regla R4**, junto a R1, R2 y R3 (`common/r4.py`).

- **Mismas variables al entrenar y al puntuar.** Las 17 variables salen de un solo módulo, `common/variables.py`. El
  entrenamiento lo aplica a los mismos eventos que envía el replayer (`eventos.csv.gz`). El streaming lo aplica, en
  cada micro-batch, a la historia que ya hay en Silver para los clientes y dispositivos del lote, más el lote mismo.
- **Umbral fijo, sin mirar la prueba.** Es el puntaje que hace alertar 35 de cada 1.000 transacciones en la
  validación (último 10 % del entrenamiento): **0,155**. Se calcula con los puntajes, no con las etiquetas, y queda
  en `lightgbm_contrato.json` junto al modelo. Con R2 y R3, el total en prueba es de 42,5 alertas por cada 1.000
  pagos (escenario H), dentro del presupuesto de 50.
- **Cada alerta R4 explica su puntaje:** `detalle = {"puntaje": 0.7159, "umbral": 0.1554, "modelo": "lightgbm_contrato"}`.
  Pasa por el mismo camino que las demás: DynamoDB con escritura condicional, SNS y Silver.
- **Si no hay modelo publicado, R4 se omite** con un aviso en el log, y el streaming sigue con R1 a R3.

**Verificación de punta a punta (en local, con Kafka, Spark, MinIO y DynamoDB Local):** se reprodujeron las primeras
20.000 transacciones reales con el replayer. El streaming generó **544 alertas R4**. Calculadas por fuera con el
mismo modelo y la misma historia, también salen **544**: son las mismas transacciones y el puntaje coincide hasta el
cuarto decimal. Es decir, lo que se evaluó en F2 y F3 es exactamente lo que corre en producción.

Para activarla (en local o en AWS):

```bash
python ml/modelo_lightgbm.py      # entrena y deja el modelo y su umbral en data/modelos/ (imagen fraudlens-ml)
python -m common.r4 publicar      # los sube al lake: models/r4/ (imagen fraudlens-spark, con data/ montada)
# reiniciar el servicio streaming: carga el modelo al arrancar ("R4 activa ..." en el log)
```

**Límites conocidos:**
- La historia de cada cliente se lee completa desde Silver en cada lote. Con el volumen de la demo es aceptable. En
  producción iría en una tabla de estado por cliente (por ejemplo, en DynamoDB).
- R1 sigue activa. Para el escenario recomendado (C), hay que apagarla o dejarla solo como información en el tablero.

## Contra qué se compara

- **La realidad:** las etiquetas de fraude de Vesta. Es la comparación de esta tabla.
- **El estado del arte en el mismo dataset:** las mejores soluciones de la competencia usan modelos de *gradient
  boosting* y reportan AUC-ROC cercanas a 0,95 (verificar la cifra exacta en el leaderboard de Kaggle). Las reglas no
  dan un puntaje continuo, así que la comparación justa es contra un modelo propio evaluado igual: es la sección
  "Reglas contra modelo" (F2) y la "Comparación final" (F3).
- **La industria:** los sistemas de reglas fijas tienen fama de generar muchos falsos positivos; el 92,6 % de este
  resultado es coherente con eso.

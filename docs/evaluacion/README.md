# Evaluación de FraudLens contra la realidad (tareas D2 y F1)

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

## Contra qué se compara

- **La realidad:** las etiquetas de fraude de Vesta. Es la comparación de esta tabla.
- **El estado del arte en el mismo dataset:** las mejores soluciones de la competencia usan modelos de *gradient
  boosting* y reportan AUC-ROC cercanas a 0,95 (verificar la cifra exacta en el leaderboard de Kaggle). Las reglas no
  dan un puntaje continuo, así que la comparación justa es contra un modelo propio evaluado igual: es la tarea **F2**
  (LightGBM) y luego **F3** (comparación final y reglas filtradas por el modelo).
- **La industria:** los sistemas de reglas fijas tienen fama de generar muchos falsos positivos; el 92,6 % de este
  resultado es coherente con eso.

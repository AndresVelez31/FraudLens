# Ficha de datos: IEEE-CIS Fraud Detection

Fuente real de transacciones de FraudLens para el Entregable 2 (plan v2, tareas B1–B3).

| Campo | Valor |
|---|---|
| Origen | Competencia *IEEE-CIS Fraud Detection* (Kaggle, 2019), datos de comercio electrónico reales de **Vesta Corporation** |
| Enlace | https://www.kaggle.com/competitions/ieee-fraud-detection |
| Uso | Académico, bajo las reglas de la competencia aceptadas por el equipo en Kaggle. Los archivos no se suben al repositorio (`data/` está en `.gitignore`) |
| Archivos usados | `train_transaction.csv` (683 MB) y `train_identity.csv` (27 MB). Los `test_*` no traen etiqueta y no se usan |
| Etiqueta | `isFraud` (1 = la transacción terminó en contracargo por fraude) |
| Perfil completo | [`ieee-cis-perfil.json`](ieee-cis-perfil.json), generado con `python scripts/perfil_ieee.py --datos data/ieee-cis` |

## Cifras principales

| Medida | Valor |
|---|---|
| Transacciones | **590.540** |
| Fraudes | **20.663 (3,50 %)** |
| Período | **182 días** (`TransactionDT` en segundos desde una fecha de referencia no publicada) |
| Ritmo real promedio | 0,038 transacciones/s: para reproducir a ~50 eventos/s se acelera el tiempo unas **1.300 veces** |
| Con dispositivo (`DeviceInfo`) | 118.666 (**20,1 %**); en ese subconjunto el fraude es 7,96 % |
| Clientes aproximados | **217.850**; mediana de **1** transacción por cliente, percentil 99 de 20 |
| Monto (USD) | mediana 68,77 · p90 275 · p99 1.104 · máximo 31.937. Mediana del fraude 75,00 contra 68,50 de lo legítimo |
| Nulos | `addr1` 11,1 % · `P_emaildomain` 16,0 % · `D1` 0,2 % · `card1` 0 % |

## Señales encontradas (sirven para calibrar las reglas y para la defensa)

| Señal | Hallazgo | Implicación |
|---|---|---|
| Tipo de producto (`ProductCD`) | C: 11,7 % de fraude · S: 5,9 % · H: 4,8 % · R: 3,8 % · W: 2,0 % | Señal fuerte; candidata para el modelo y para segmentar P4 |
| Velocidad (R1) | Transacciones del mismo cliente en 5 min: p50 1, p99 3, p99,9 13. Fraude 8,7 % si hay 3 o más, 3,2 % si hay 1 | Hay señal, pero el umbral del E1 (más de 5) casi nunca se cumple: hay que recalibrar |
| Dispositivo nuevo (R2) | Dispositivo nunca visto para un cliente con historia: 7,7 % de fraude contra 7,2 % del resto | **Casi no discrimina** en este dataset; además solo el 20 % trae dispositivo |
| Dominio de correo | `mail.com` 19,0 % · `outlook.com` 9,5 % · `hotmail.com` 5,3 % · `gmail.com` 4,4 % | Candidato para la lista de riesgo (R3) |
| Hora (`TransactionDT` mod 24 h) | El fraude sube entre las horas 5 y 9 (pico 10,6 %) | La hora real es desconocida: sirve como patrón relativo, no como franja horaria de Colombia |

## Supuestos del mapeo al contrato de FraudLens

- **Cliente:** IEEE-CIS no trae un id de cliente. Se aproxima con `card1 + addr1 + D1n`, donde `D1n` = día de la
  transacción − `D1` (el día de alta de la tarjeta). Es la técnica usada por las mejores soluciones de la competencia;
  es una aproximación y puede juntar o separar titulares.
- **Tiempo:** `ts_evento` = fecha base fija + `TransactionDT`. Se conservan los intervalos reales entre transacciones.
- **Moneda:** los montos están en USD; los umbrales en COP del E1 no aplican y se recalibran con estos datos.
- **No hay comercio ni ciudad reales:** el dataset está anonimizado. El mapeo propuesto usa el tipo de producto y la
  región (`addr1`) como sustitutos; está documentado en el plan (tarea B2).
- **No hay datos colombianos:** no existe un dataset público real de billeteras colombianas con fraude etiquetado.
  El contexto colombiano (Bre-B, Asobancaria) se usa solo para el dimensionamiento.

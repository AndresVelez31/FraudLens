# Evaluación de las reglas contra etiquetas reales (IEEE-CIS)

Partición de **prueba** (20 % final en el tiempo, 118108 transacciones), tasa base de fraude **3.44 %**. Generado con `python -m batch.evaluacion evaluar`.

| Versión | Regla | Alertadas | Precisión | Cobertura (recall) | F1 | Falsos positivos | Alertas/1.000 | Monto de fraude detectado |
|---|---|---|---|---|---|---|---|---|
| v1 | R1 | 1294 | 0.0 % | 0.0 % | 0.000 | 1294 (1.13 %) | 11.0 | 0.0 % |
| v1 | R2 | 262 | 14.5 % | 0.9 % | 0.018 | 224 (0.20 %) | 2.2 | 0.9 % |
| v1 | R3 | 533 | 18.6 % | 2.4 % | 0.043 | 434 (0.38 %) | 4.5 | 1.1 % |
| v1 | Cualquiera (R1, R2 o R3) | 2076 | 6.5 % | 3.3 % | 0.044 | 1941 (1.70 %) | 17.6 | 1.9 % |
| ieee | R1 | 4303 | 5.3 % | 5.6 % | 0.054 | 4075 (3.57 %) | 36.4 | 4.1 % |
| ieee | R2 | 749 | 13.5 % | 2.5 % | 0.042 | 648 (0.57 %) | 6.3 | 1.9 % |
| ieee | R3 | 533 | 18.6 % | 2.4 % | 0.043 | 434 (0.38 %) | 4.5 | 1.1 % |
| ieee | Cualquiera (R1, R2 o R3) | 5479 | 7.4 % | 10.0 % | 0.086 | 5071 (4.45 %) | 46.4 | 6.7 % |

# Comparación final: reglas, modelo y combinaciones (F3)

Partición de **prueba** de IEEE-CIS: 118108 transacciones, tasa base de fraude **3.44 %**. Generado con `python ml/comparacion_final.py`.

| Escenario | Alertas/1.000 | Precisión | Cobertura | F1 | Falsos positivos | FP por 1.000 legítimas | Monto de fraude detectado |
|---|---|---|---|---|---|---|---|
| Reglas calibradas (R1, R2 o R3) | 46.4 | 7.4 % | 10.0 % | 0.086 | 5071 | 44.5 | 6.7 % |
| Modelo "contrato" (mismo presupuesto) | 46.4 | 20.9 % | 28.2 % | 0.240 | 4334 | 38.0 | 27.1 % |
| Reglas filtradas por el modelo (se conserva el 75 %) | 34.8 | 9.0 % | 9.1 % | 0.090 | 3739 | 32.8 | 6.0 % |
| Reglas filtradas por el modelo (se conserva el 50 %) | 23.2 | 12.1 % | 8.2 % | 0.098 | 2408 | 21.1 | 5.0 % |
| Reglas filtradas por el modelo (se conserva el 25 %) | 11.6 | 18.5 % | 6.2 % | 0.093 | 1116 | 9.8 | 4.1 % |
| R2 y R3 + modelo (sin R1), mismo presupuesto | 46.4 | 19.8 % | 26.6 % | 0.227 | 4397 | 38.6 | 25.7 % |
| Mitad de las reglas con mayor puntaje + modelo, mismo presupuesto | 46.4 | 17.7 % | 23.8 % | 0.203 | 4510 | 39.5 | 24.0 % |
| Streaming (D5): R2, R3 y R4 con umbral fijo 0.155 | 42.5 | 19.9 % | 24.5 % | 0.220 | 4022 | 35.3 | 24.4 % |
| Modelo "vesta" (referencia, mismo presupuesto) | 46.4 | 43.0 % | 58.0 % | 0.494 | 3123 | 27.4 | 55.3 % |

Precisión de cada regla, sola y quedándose con la mitad de sus alertas de mayor puntaje:

| Regla | Alertas | Precisión | Mitad con mayor puntaje: precisión | Cobertura que conserva |
|---|---|---|---|---|
| R1 | 4303 | 5.3 % | 7.8 % | 73 % de sus aciertos |
| R2 | 749 | 13.5 % | 21.9 % | 81 % de sus aciertos |
| R3 | 533 | 18.6 % | 29.3 % | 79 % de sus aciertos |

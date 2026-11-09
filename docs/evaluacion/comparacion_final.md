# Comparación final: reglas, modelo y combinaciones (F3)

Partición de **prueba** de IEEE-CIS: 118108 transacciones, tasa base de fraude **3.44 %**. Generado con `python ml/comparacion_final.py`.

| Escenario | Alertas/1.000 | Precisión | Cobertura | F1 | Falsos positivos | FP por 1.000 legítimas | Monto de fraude detectado |
|---|---|---|---|---|---|---|---|
| Reglas calibradas (R1, R2 o R3) | 46.4 | 7.4 % | 10.0 % | 0.086 | 5071 | 44.5 | 6.7 % |
| Modelo "contrato" (mismo presupuesto) | 46.4 | 20.6 % | 27.8 % | 0.236 | 4351 | 38.1 | 26.6 % |
| Reglas filtradas por el modelo (se conserva el 75 %) | 34.8 | 9.0 % | 9.1 % | 0.090 | 3740 | 32.8 | 6.0 % |
| Reglas filtradas por el modelo (se conserva el 50 %) | 23.2 | 11.0 % | 7.4 % | 0.088 | 2439 | 21.4 | 4.6 % |
| Reglas filtradas por el modelo (se conserva el 25 %) | 11.6 | 16.6 % | 5.6 % | 0.084 | 1142 | 10.0 | 3.6 % |
| R2 y R3 + modelo (sin R1), mismo presupuesto | 46.4 | 19.6 % | 26.4 % | 0.225 | 4406 | 38.6 | 26.2 % |
| Mitad de las reglas con mayor puntaje + modelo, mismo presupuesto | 46.4 | 17.3 % | 23.3 % | 0.199 | 4532 | 39.7 | 23.6 % |
| Modelo "vesta" (referencia, mismo presupuesto) | 46.4 | 43.0 % | 58.0 % | 0.494 | 3123 | 27.4 | 55.3 % |

Precisión de cada regla, sola y quedándose con la mitad de sus alertas de mayor puntaje:

| Regla | Alertas | Precisión | Mitad con mayor puntaje: precisión | Cobertura que conserva |
|---|---|---|---|---|
| R1 | 4303 | 5.3 % | 6.8 % | 64 % de sus aciertos |
| R2 | 749 | 13.5 % | 21.7 % | 80 % de sus aciertos |
| R3 | 533 | 18.6 % | 30.4 % | 82 % de sus aciertos |

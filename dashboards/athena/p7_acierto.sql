-- P7: ¿Qué tanto aciertan las alertas contra la realidad? Precisión (de cada 100 alertas, cuántas eran fraude),
-- cobertura (de cada 100 fraudes, cuántos se marcaron) y falsos positivos por cada 1.000 pagos legítimos.
-- Por partición de evaluación: la honesta es "prueba" (el 20 % final, que no se usó para calibrar ni entrenar).
-- Solo transacciones con etiqueta. Una transacción con varias alertas cuenta una vez.
SELECT
  t.particion_eval,
  COUNT(*)                                                                            AS n_transacciones,
  SUM(CASE WHEN t.es_fraude_real THEN 1 ELSE 0 END)                                   AS n_fraude_real,
  SUM(CASE WHEN t.es_sospechosa THEN 1 ELSE 0 END)                                    AS n_alertadas,
  SUM(CASE WHEN t.es_sospechosa AND t.es_fraude_real THEN 1 ELSE 0 END)               AS aciertos,
  SUM(CASE WHEN t.es_sospechosa AND NOT t.es_fraude_real THEN 1 ELSE 0 END)           AS falsos_positivos,
  ROUND(100.0 * SUM(CASE WHEN t.es_sospechosa AND t.es_fraude_real THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN t.es_sospechosa THEN 1 ELSE 0 END), 0), 2)             AS precision_pct,
  ROUND(100.0 * SUM(CASE WHEN t.es_sospechosa AND t.es_fraude_real THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN t.es_fraude_real THEN 1 ELSE 0 END), 0), 2)            AS cobertura_pct,
  ROUND(1000.0 * SUM(CASE WHEN t.es_sospechosa AND NOT t.es_fraude_real THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN NOT t.es_fraude_real THEN 1 ELSE 0 END), 0), 2)        AS fp_por_mil_legitimas
FROM fact_transacciones t
WHERE t.es_fraude_real IS NOT NULL
GROUP BY t.particion_eval
ORDER BY t.particion_eval

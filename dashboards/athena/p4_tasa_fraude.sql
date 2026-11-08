-- P4: ¿Cuál es la tasa de fraude (% de transacciones y % del monto) por comercio, ciudad y franja horaria, semana a semana?
-- Funciona igual en Amazon Athena (tablas Delta registradas en Glue) y en Spark SQL (batch/consultas.py).
SELECT
  date_trunc('week', t.ts_evento)                                              AS semana,
  m.id_comercio,
  m.ciudad,
  tm.franja,
  COUNT(*)                                                                      AS n_transacciones,
  SUM(CASE WHEN t.es_sospechosa THEN 1 ELSE 0 END)                              AS n_sospechosas,
  ROUND(100.0 * SUM(CASE WHEN t.es_sospechosa THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_transacciones,
  ROUND(100.0 * SUM(CASE WHEN t.es_sospechosa THEN t.monto ELSE 0 END) / SUM(t.monto), 2) AS pct_monto
FROM fact_transacciones t
JOIN dim_comercio m  ON t.id_comercio = m.id_comercio
JOIN dim_tiempo   tm ON t.id_tiempo   = tm.id_tiempo
GROUP BY date_trunc('week', t.ts_evento), m.id_comercio, m.ciudad, tm.franja
ORDER BY semana, pct_monto DESC, n_sospechosas DESC

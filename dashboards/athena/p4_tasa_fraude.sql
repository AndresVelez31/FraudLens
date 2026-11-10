-- P4: ¿Cuál es la tasa de fraude (% de transacciones y % del monto) por línea de producto, región y franja horaria,
-- semana a semana? Con IEEE-CIS el "comercio" es la línea de producto (W, C, R, H, S) y la región es la de
-- facturación del pago (addr1), que viaja en la transacción. La franja está en hora de Colombia (dim_tiempo).
-- "Sospechosa" = la marcó alguna regla; "fraude real" = etiqueta confirmada (vacía si no hay etiquetas).
-- Funciona igual en Amazon Athena (tablas Delta registradas en Glue) y en Spark SQL (batch/consultas.py).
SELECT
  date_trunc('week', tm.fecha)                                                  AS semana,
  m.id_comercio                                                                 AS producto,
  t.region,
  tm.franja,
  COUNT(*)                                                                      AS n_transacciones,
  SUM(CASE WHEN t.es_sospechosa THEN 1 ELSE 0 END)                              AS n_sospechosas,
  ROUND(100.0 * SUM(CASE WHEN t.es_sospechosa THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_transacciones,
  ROUND(100.0 * SUM(CASE WHEN t.es_sospechosa THEN t.monto ELSE 0 END) / SUM(t.monto), 2) AS pct_monto,
  ROUND(100.0 * SUM(CASE WHEN t.es_fraude_real THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_fraude_real
FROM fact_transacciones t
JOIN dim_comercio m  ON t.id_comercio = m.id_comercio
JOIN dim_tiempo   tm ON t.id_tiempo   = tm.id_tiempo
GROUP BY date_trunc('week', tm.fecha), m.id_comercio, t.region, tm.franja
HAVING COUNT(*) >= 10   -- grupos muy pequeños no dicen nada (hay ~300 regiones)
ORDER BY semana, pct_monto DESC, n_sospechosas DESC

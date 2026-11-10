-- P6: ¿Cuáles son las 10 regiones (con su línea de producto) con mayor monto sospechoso en los últimos 7 días?
-- Los 7 días se cuentan desde el último pago registrado, no desde la hora del servidor: así la consulta también
-- responde al reproducir un histórico (en operación normal es lo mismo).
SELECT
  t.region,
  m.id_comercio           AS producto,
  COUNT(*)                AS n_sospechosas,
  ROUND(SUM(t.monto), 2)  AS monto_sospechoso,
  SUM(CASE WHEN t.es_fraude_real THEN 1 ELSE 0 END) AS n_fraude_real
FROM fact_transacciones t
JOIN dim_comercio m ON t.id_comercio = m.id_comercio
WHERE t.es_sospechosa
  AND t.ts_evento >= (SELECT MAX(ts_evento) FROM fact_transacciones) - INTERVAL '7' DAY
GROUP BY t.region, m.id_comercio
ORDER BY monto_sospechoso DESC
LIMIT 10

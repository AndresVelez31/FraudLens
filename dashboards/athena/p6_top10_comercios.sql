-- P6: ¿Cuáles son los 10 comercios con mayor monto sospechoso en los últimos 7 días?
SELECT
  m.id_comercio,
  m.nombre,
  m.ciudad,
  COUNT(*)                AS n_sospechosas,
  ROUND(SUM(t.monto), 2)  AS monto_sospechoso
FROM fact_transacciones t
JOIN dim_comercio m ON t.id_comercio = m.id_comercio
WHERE t.es_sospechosa
  AND t.ts_evento >= current_timestamp - INTERVAL '7' DAY
GROUP BY m.id_comercio, m.nombre, m.ciudad
ORDER BY monto_sospechoso DESC
LIMIT 10

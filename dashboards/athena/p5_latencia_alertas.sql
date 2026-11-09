-- P5: ¿Cuál es la latencia promedio y el percentil 95 entre la transacción y la alerta, y cuántas alertas dispara cada regla?
-- (solo alertas del streaming: el reproceso no tiene latencia real). En Spark SQL batch/consultas.py cambia
-- approx_percentile por percentile_approx, que es el mismo cálculo con otro nombre.
SELECT
  r.id_regla,
  r.nombre                                    AS regla,
  a.rule_version,
  COUNT(*)                                    AS n_alertas,
  ROUND(AVG(a.latencia_s), 2)                 AS latencia_prom_s,
  ROUND(approx_percentile(a.latencia_s, 0.95), 2) AS latencia_p95_s,
  ROUND(MAX(a.latencia_s), 2)                 AS latencia_max_s
FROM fact_alertas a
JOIN dim_regla r ON a.id_regla = r.id_regla
WHERE a.origen = 'streaming'
GROUP BY r.id_regla, r.nombre, a.rule_version
ORDER BY r.id_regla, a.rule_version

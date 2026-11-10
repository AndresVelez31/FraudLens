-- P5: ¿Cuál es la latencia promedio y el percentil 95 entre la transacción y la alerta, y cuántas alertas dispara cada regla?
-- La latencia se mide desde que el pago se EMITE (ts_emision; o desde que llega a Kafka), no desde ts_evento: al
-- reproducir un histórico, ts_evento es de meses atrás. Se parte en tramos (batch/gold.py, D4):
--   envío = Kafka - emisión · cola = espera en Kafka hasta el lote del streaming · reglas = lote -> alerta.
-- Solo alertas del streaming (el reproceso no tiene latencia real). En Spark SQL batch/consultas.py cambia
-- approx_percentile por percentile_approx, que es el mismo cálculo con otro nombre.
SELECT
  r.id_regla,
  r.nombre                                         AS regla,
  a.rule_version,
  COUNT(*)                                         AS n_alertas,
  ROUND(AVG(a.latencia_s), 2)                      AS latencia_prom_s,
  ROUND(approx_percentile(a.latencia_s, 0.95), 2)  AS latencia_p95_s,
  ROUND(MAX(a.latencia_s), 2)                      AS latencia_max_s,
  ROUND(AVG(a.lat_envio_s), 2)                     AS envio_prom_s,
  ROUND(AVG(a.lat_cola_s), 2)                      AS cola_prom_s,
  ROUND(AVG(a.lat_reglas_s), 2)                    AS reglas_prom_s
FROM fact_alertas a
JOIN dim_regla r ON a.id_regla = r.id_regla
WHERE a.origen = 'streaming'
GROUP BY r.id_regla, r.nombre, a.rule_version
ORDER BY r.id_regla, a.rule_version

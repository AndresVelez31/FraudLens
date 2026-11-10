-- P7 (detalle): precisión de cada regla que marca una transacción concreta (R2, R3, R4). R1 marca una ventana de
-- 5 minutos y no una transacción, así que su acierto se ve en el total (p7_acierto.sql).
SELECT
  a.id_regla,
  r.nombre                                                                AS regla,
  a.rule_version,
  a.particion_eval,
  COUNT(DISTINCT a.id_transaccion)                                        AS n_alertas,
  COUNT(DISTINCT CASE WHEN t.es_fraude_real THEN a.id_transaccion END)    AS aciertos,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN t.es_fraude_real THEN a.id_transaccion END)
        / COUNT(DISTINCT a.id_transaccion), 2)                            AS precision_pct
FROM fact_alertas a
JOIN fact_transacciones t ON a.id_transaccion = t.id_transaccion
JOIN dim_regla r          ON a.id_regla = r.id_regla
WHERE t.es_fraude_real IS NOT NULL
GROUP BY a.id_regla, r.nombre, a.rule_version, a.particion_eval
ORDER BY a.particion_eval, a.id_regla

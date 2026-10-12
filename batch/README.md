# batch

**Responsable:** Sebastián Salazar Henao · **Par revisor:** Hellen Yanes Doria

Extracción JDBC de PostgreSQL y carga de CSV a Bronze, Silver de catálogos, Gold en esquema en estrella
(dimensiones SCD2 y hechos), consultas P4–P6, registro en Glue, OPTIMIZE ZORDER, reproceso por `rule_version` y HMAC.

Requisitos: RF-02, RF-03, RF-10 a RF-13, RF-16, RNF-06, RNF-07.

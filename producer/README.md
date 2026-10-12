# producer

**Responsable:** Andrés Felipe Vélez Álvarez · **Par revisor:** Samuel Samper Cardona

Productor de transacciones en Python + Faker: publica eventos JSON en el topic `transacciones` con clave `id_cliente`,
`acks=all` e idempotencia. Incluye escenarios de fraude (R1–R3), duplicados y JSON malformado con semilla fija.

Requisitos: RF-01.

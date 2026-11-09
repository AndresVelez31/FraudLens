"""Contrato de datos del evento de transacción (se cambia solo por PR aprobado por los dueños afectados).

Evento JSON publicado en el topic `transacciones` (clave de Kafka = id_cliente):

    {"id_transaccion": "uuid", "ts_evento": "2026-11-04T14:00:00.123Z", "id_cliente": "C000123",
     "id_comercio": "M0042", "id_dispositivo": "D000123-1", "monto": 85000.0,
     "tipo": "QR|TRANSFERENCIA|COMERCIO", "ciudad": "Medellín",
     "documento": "...", "telefono": "...", "correo": "..."}

Con datos reales de IEEE-CIS (producer/replayer.py), `tipo` es el tipo de producto (W, C, R, H, S), `id_comercio` es ese
mismo producto, `ciudad` es la región anonimizada (`addr1`) e `id_dispositivo` puede venir vacío: solo el 20 % de las
transacciones trae dispositivo. El replayer agrega `ts_emision`, `region` y `correo_dominio`, que por ahora el
streaming ignora (ver docs/datos/ieee-cis.md).
"""
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

TIPOS_SINTETICOS = ["QR", "TRANSFERENCIA", "COMERCIO"]   # productor sintético (producer/producer.py)
TIPOS_IEEE = ["W", "C", "R", "H", "S"]                   # ProductCD de IEEE-CIS
TIPOS_VALIDOS = TIPOS_SINTETICOS + TIPOS_IEEE
CORRUPTO = "_registro_corrupto"

# Todos los campos llegan como texto/número tal cual; la validación decide si sirven.
EVENTO = StructType(
    [
        StructField("id_transaccion", StringType()),
        StructField("ts_evento", StringType()),
        StructField("id_cliente", StringType()),
        StructField("id_comercio", StringType()),
        StructField("id_dispositivo", StringType()),
        StructField("monto", DoubleType()),
        StructField("tipo", StringType()),
        StructField("ciudad", StringType()),
        StructField("documento", StringType()),
        StructField("telefono", StringType()),
        StructField("correo", StringType()),
        StructField(CORRUPTO, StringType()),
    ]
)

# id_dispositivo es opcional: si fuera obligatorio, el 80 % de las transacciones reales iría a cuarentena.
CAMPOS_OBLIGATORIOS = ["id_transaccion", "ts_evento", "id_cliente", "id_comercio", "monto", "tipo"]

"""Seudonimización de datos personales (RNF-07, Ley 1581 de 2012).

Se usa HMAC-SHA-256 con una clave que NO está en el repositorio. Un hash simple (SHA-256 sin clave) de un
documento o un teléfono se revierte probando todos los valores posibles; con HMAC el atacante necesita la clave.

El HMAC se calcula con funciones nativas de Spark (sin UDF de Python, que añadía ~2 s por micro-batch):
    HMAC(K, m) = SHA256( (K xor opad) || SHA256( (K xor ipad) || m ) )
y se verifica contra la librería estándar `hmac` en tests/test_pii.py.
"""
import hashlib
import hmac

from pyspark.sql import functions as F

from common import config

_BLOQUE = 64  # tamaño de bloque de SHA-256


def hmac_hex(valor, clave):
    """Referencia en Python puro (la usan las pruebas y quien necesite un valor suelto)."""
    if valor is None:
        return None
    return hmac.new(clave.encode(), str(valor).strip().lower().encode(), hashlib.sha256).hexdigest()


def _rellenos(clave):
    k = clave.encode()
    if len(k) > _BLOQUE:
        k = hashlib.sha256(k).digest()
    k = k.ljust(_BLOQUE, b"\x00")
    return bytearray(b ^ 0x36 for b in k), bytearray(b ^ 0x5C for b in k)


def hmac_col(columna):
    """Columna de Spark con el HMAC-SHA-256 (hex) de otra columna. Nulo se mantiene nulo."""
    ipad, opad = _rellenos(config.pii_key())
    mensaje = F.encode(F.lower(F.trim(columna.cast("string"))), "utf-8")
    interno = F.unhex(F.sha2(F.concat(F.lit(ipad), mensaje), 256))
    return F.sha2(F.concat(F.lit(opad), interno), 256)

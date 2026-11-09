import os
import sys

import pytest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)
os.environ.setdefault("PII_HMAC_KEY", "clave-de-prueba")
os.environ.setdefault("TZ", "UTC")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from common.spark import get_spark

    os.environ.setdefault("LAKE_URI", str(tmp_path_factory.mktemp("lake")))
    s = get_spark("fraudlens-tests", shuffle_partitions=2)
    yield s
    s.stop()

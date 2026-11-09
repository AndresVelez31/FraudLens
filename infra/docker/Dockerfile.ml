# Imagen para entrenar y evaluar el modelo de referencia (tarea F2), fuera del pipeline de streaming.
#   docker build -f infra/docker/Dockerfile.ml -t fraudlens-ml .
#   docker run --rm -v "$PWD:/opt/fraudlens" fraudlens-ml python ml/modelo_lightgbm.py
FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
COPY infra/docker/requirements-ml.txt /tmp/requirements-ml.txt
RUN pip install --no-cache-dir -r /tmp/requirements-ml.txt
WORKDIR /opt/fraudlens
ENV PYTHONUNBUFFERED=1

#!/bin/bash
# User data de las EC2 de FraudLens (Amazon Linux 2023, x86_64). Se pega en
# "Advanced details > User data" al lanzar la instancia y corre una sola vez, como root, en el primer arranque.
# Deja Docker, Compose, Buildx y el repositorio listos en /opt/fraudlens. Registro: /var/log/fraudlens-user-data.log
set -euxo pipefail
exec > /var/log/fraudlens-user-data.log 2>&1

RAMA="main"                # las EC2 siempre corren lo que está fusionado en main

dnf install -y docker git
systemctl enable --now docker
usermod -aG docker ec2-user
getent passwd ssm-user >/dev/null && usermod -aG docker ssm-user || true

# Plugins de Docker con versión fija (el paquete de AL2023 no los trae)
PLUGINS=/usr/local/lib/docker/cli-plugins
mkdir -p "$PLUGINS"
curl -fsSL https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64 -o "$PLUGINS/docker-compose"
curl -fsSL https://github.com/docker/buildx/releases/download/v0.17.1/buildx-v0.17.1.linux-amd64 -o "$PLUGINS/docker-buildx"
chmod +x "$PLUGINS/docker-compose" "$PLUGINS/docker-buildx"

git clone --branch "$RAMA" https://github.com/AndresVelez31/FraudLens.git /opt/fraudlens
chown -R ec2-user:ec2-user /opt/fraudlens
# El repo es de ec2-user, pero por Session Manager se trabaja como root (sudo -i): sin esto git lo rechaza
# ("detected dubious ownership") y no se puede hacer git pull.
git config --system --add safe.directory /opt/fraudlens

docker --version
docker compose version
echo "FraudLens: user data terminado"

#!/usr/bin/env bash
#
# One-time local infrastructure setup. Requires sudo (installs RabbitMQ and
# creates the PostgreSQL role/database). Re-runnable: each step is idempotent.
#
#   ./setup.sh
#
set -euo pipefail

echo "==> Installing RabbitMQ"
sudo apt-get update
sudo apt-get install -y rabbitmq-server
sudo systemctl start rabbitmq-server
sudo systemctl enable rabbitmq-server
sudo rabbitmq-plugins enable rabbitmq_management
echo "    RabbitMQ management UI: http://localhost:15672 (guest/guest)"

echo "==> Creating PostgreSQL role 'admin' and database 'orders'"
sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='admin'" | grep -q 1 \
  || sudo -u postgres psql -c "CREATE USER admin WITH PASSWORD 'admin';"
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='orders'" | grep -q 1 \
  || sudo -u postgres psql -c "CREATE DATABASE orders OWNER admin;"

echo "==> Done. Now run: npm install"

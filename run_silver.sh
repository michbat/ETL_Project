#!/usr/bin/env bash
set -euo pipefail

POSTGRES_HOST="${POSTGRES_HOST:-postgres-db}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"
POSTGRES_DB="${POSTGRES_DB:-}"

if [ -z "${POSTGRES_USER}" ] || [ -z "${POSTGRES_PASSWORD}" ] || [ -z "${POSTGRES_DB}" ]; then
    echo "ERROR: environment variables POSTGRES_USER, POSTGRES_PASSWORD and POSTGRES_DB must be set" >&2
    exit 1
fi

export POSTGRES_HOST POSTGRES_PORT POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB

echo "Début de la transformation avec host=${POSTGRES_HOST} port=${POSTGRES_PORT} db=${POSTGRES_DB}"

python silver_state_city_reference.py
python silver_state_ethnicity_reference.py
python silver_shooting_reference.py
python silver_shooting_enriched.py

echo "Tous les scripts de transformation Silver ont été exécutés avec succès!"
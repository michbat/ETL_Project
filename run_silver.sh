#!/usr/bin/env bash
set -euo pipefail

# Affecter les variables d'environnement avec des valeurs par défaut si elles ne sont pas définies
POSTGRES_HOST="${POSTGRES_HOST:-postgres-db}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"
POSTGRES_DB="${POSTGRES_DB:-}"

# Vérifier que les variables d'environnement nécessaires sont définies
if [ -z "${POSTGRES_USER}" ] || [ -z "${POSTGRES_PASSWORD}" ] || [ -z "${POSTGRES_DB}" ]; then
    echo "ERREUR: les variables d'environnement POSTGRES_USER, POSTGRES_PASSWORD et POSTGRES_DB doivent être définies" >&2
    exit 1
fi

# Exporter les variables d'environnement pour qu'elles soient accessibles aux scripts Python
export POSTGRES_HOST POSTGRES_PORT POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB

echo "Début de la transformation avec host=${POSTGRES_HOST} port=${POSTGRES_PORT} db=${POSTGRES_DB}"

# Exécuter les scripts de transformation Silver
python silver_state_city_reference.py
python silver_state_ethnicity_reference.py
python silver_shooting_reference.py
python silver_shooting_enriched.py

echo "Tous les scripts de transformation Silver ont été exécutés avec succès!"
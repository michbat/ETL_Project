#!/usr/bin/env bash
set -euo pipefail  # Exit on error, undefined variable, or error in pipeline


# Affecter les variables d'environnement avec des valeurs par défaut si elles ne sont pas définies
POSTGRES_HOST="${POSTGRES_HOST:-}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"
POSTGRES_DB="${POSTGRES_DB:-}"

# Vérifier que les variables d'environnement nécessaires sont définies
if [ -z "${POSTGRES_USER}" ] || [ -z "${POSTGRES_PASSWORD}" ] || [ -z "${POSTGRES_DB}" ]; then
	echo "ERROR: environment variables POSTGRES_USER, POSTGRES_PASSWORD and POSTGRES_DB must be set" >&2
	exit 1
fi

# Construire les arguments de connexion à PostgreSQL à partir des variables d'environnement
PG_ARGS=(--pg-host "${POSTGRES_HOST}" --pg-port "${POSTGRES_PORT}" --pg-user "${POSTGRES_USER}" --pg-pass "${POSTGRES_PASSWORD}" --pg-db "${POSTGRES_DB}")


echo "Début de l'ingestion avec host=${POSTGRES_HOST} port=${POSTGRES_PORT} db=${POSTGRES_DB}"

python ingestion_cities_data.py "${PG_ARGS[@]}"
python ingestion_ethnicity_data.py "${PG_ARGS[@]}"
python ingestion_shootings_data.py "${PG_ARGS[@]}"

echo "Tous les scripts d'ingestion ont été exécutés avec succès!"

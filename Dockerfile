FROM python:3.13.10-slim

# Copier le binaire uv depuis l'image officielle (multi-stage build)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Définir le répertoire de travail
WORKDIR /app

# Ajouter l'environnement virtuel au PATH pour utiliser les packages installés
ENV PATH="/app/.venv/bin:$PATH"

# Copier d'abord les fichiers de dépendances (optimisation du cache Docker)
COPY pyproject.toml uv.lock .python-version ./

# Installer les dépendances système nécessaires (libpq pour psycopg, outils de build)
RUN apt-get update \
	&& apt-get install -y --no-install-recommends libpq-dev libpq5 build-essential gcc ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

# Créer un environnement virtuel dans l'image en utilisant uv et le lockfile
# (ne pas copier .venv depuis l'hôte: souvent incompatible)
RUN uv sync --locked

# Copier les scripts d'ingestion
COPY ./Bronze_Stagging_Python_Scripts/ingestion_cities_data.py ./
COPY ./Bronze_Stagging_Python_Scripts/ingestion_ethnicity_data.py ./
COPY ./Bronze_Stagging_Python_Scripts/ingestion_shootings_data.py ./

# Ne pas copier les données dans l'image — on montera le dossier `datasets` au runtime
# (évite d'embarquer des données volumineuses et permet de mettre à jour les sources sans rebuild)

# Copier le runner shell et le rendre exécutable
COPY run_all.sh /app/run_all.sh
RUN chmod +x /app/run_all.sh

# Point d'entrée: lancer le runner shell qui exécute les 3 scripts
ENTRYPOINT ["/app/run_all.sh"]

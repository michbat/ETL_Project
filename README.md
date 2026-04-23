# Projet ETL - Architecture Médaillon

## Présentation

Ce projet met en œuvre une architecture ETL basée sur le pattern "médaillon" (Bronze, Silver, Gold) pour l'ingestion, la transformation et la valorisation de données sur les violences policières aux États-Unis. L'orchestration s'appuie sur Docker et Docker Compose pour garantir la portabilité, la reproductibilité et l'isolation des environnements.

---

## 1. Mise en place de l'environnement Python

- Créez un environnement virtuel Python :
	```bash
	python3 -m venv .venv
	```
- Installez le gestionnaire de dépendances `uv` et initialisez le projet :
	```bash
	uv pip install --system uv
	uv init
	```
- Les dépendances sont définies dans `pyproject.toml` et verrouillées dans `uv.lock`.

---

## 2. Organisation du projet

- **Bronze_Stagging_Notebooks/** et **Bronze_Stagging_Python_Scripts/** : Notebooks et scripts pour l'ingestion des données brutes (cities, ethnicity, shootings).
- **Silver_Stagging_Notebooks/** et **Silver_Stagging_Python_Scripts/** : Notebooks et scripts pour la création des tables de référence enrichies.
- **Gold_Stagging_Notebooks/** et **Gold_Stagging_Python_Scripts/** : Notebooks et scripts pour la création des tables dimensionnelles et de faits.
- **datasets/** : Contient les fichiers de données sources (CSV, Excel).

Chaque notebook correspond à une étape de transformation, puis est converti en script Python pour l'orchestration automatisée.

---

## 3. Orchestration avec Docker et Docker Compose

- Le fichier `docker-compose.yaml` définit les services suivants :
	- **postgres-db** : Instance PostgreSQL pour le stockage des données.
	- **bronze**, **silver**, **gold** : Services pour chaque couche de l'ETL, chacun construit à partir de son propre Dockerfile.
- Un réseau interne (`pg-network`) permet la communication sécurisée entre les conteneurs.
- Les variables sensibles (identifiants de la base) sont centralisées dans le fichier `etl.env`.

---

## 4. Scripts d'orchestration

- `run_bronze.sh`, `run_silver.sh`, `run_gold.sh` : Scripts shell qui servent de point d'entrée pour chaque image Docker. Ils valident la présence des variables d'environnement nécessaires et orchestrent l'exécution séquentielle des scripts Python de chaque couche.

---

## 5. Dockerfiles spécialisés

- **Dockerfile.bronze**, **Dockerfile.silver**, **Dockerfile.gold** : Chaque Dockerfile construit une image adaptée à la couche correspondante, installe les dépendances, copie les scripts et définit le script d'orchestration comme entrypoint.

---

## 6. Exécution

- Construction des images :
	```bash
	docker build -f Dockerfile.bronze -t bronze .
	docker build -f Dockerfile.silver -t silver .
	docker build -f Dockerfile.gold -t gold .
	```
- Lancement des services via Docker Compose :
	```bash
	docker compose up bronze
	docker compose up silver
	docker compose up gold
	```
- Ou exécution manuelle d'un service :
	```bash
	docker run --env-file etl.env --rm --network pg-network bronze
	```

---

## 7. Sécurité

- Ne jamais versionner de vrais identifiants dans `etl.env`.
- `.gitignore` est configuré pour ignorer `.venv` et les fichiers sensibles.

---

## 8. Notes complémentaires

- Les notebooks facilitent le prototypage et la documentation des transformations (une sorte de brouillon interactif).
- Les scripts Python sont utilisés pour l'automatisation dans les conteneurs.




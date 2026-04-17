# Projet d'ingestion — DataEngineering / Didier

Ce dépôt contient des notebooks et scripts pour ingérer des jeux de données CSV/Excel
vers une base PostgreSQL. Les principales tâches réalisées sont listées ci-dessous,
avec des exemples d'utilisation et des remarques opérationnelles.

**Résumé des modifications**
- Notebooks modifiés sous `Bronze_Stagging_Notebooks/` pour ajouter des colonnes
	d'ingestion (`source_filename`, `batch_id`, `load_datetime`) et garantir
	la création correcte du schéma/table lors de la première ingestion.
- Nouveaux scripts Python créés à partir des notebooks:
	- `ingestion_ethnicity_data.py` — ingestion Excel (Ethnicity_Data_Usa.xlsx)
	- `ingestion_cities_data.py` — ingestion chunked pour `datasets/uscities.csv`
	- `ingestion_shootings_data.py` — ingestion chunked pour
		`datasets/fatal-police-shootings-data.csv`

**Comportement important**
- Les scripts ajoutent automatiquement trois colonnes d'ingestion à chaque
	enregistrement: `source_filename` (VARCHAR), `batch_id` (VARCHAR),
	`load_datetime` (TIMESTAMP). Ces colonnes sont incluses lors de la création
	initiale de la table afin d'éviter des erreurs `UndefinedColumn` lors de
	l'append.
- Pour les gros CSV (cities, shootings), l'ingestion se fait par chunks
	(lecture itérative) pour économiser la mémoire. Le premier chunk est utilisé
	pour créer la structure de la table (`df.head(0).to_sql(..., if_exists='replace')`),
	puis les chunks suivants sont appendés.

Fichiers clés
- `ingestion_ethnicity_data.py` — ingestion Excel, normalisation, coercion,
	ajout métadonnées, création table puis insertion.
- `ingestion_cities_data.py` — ingestion chunked pour `datasets/uscities.csv`.
- `ingestion_shootings_data.py` — ingestion chunked pour
	`datasets/fatal-police-shootings-data.csv`.
- Notebooks d'origine: `Bronze_Stagging_Notebooks/ingestion_*.ipynb`.

Prérequis
- Python 3.9+ (ou équivalent utilisé dans l'environnement virtuel)
- Packages Python utilisés: `pandas`, `sqlalchemy`, `psycopg`/`psycopg2`,
	`click`, `tqdm`. (Ajouter ces packages dans votre `pyproject.toml` ou
	`requirements.txt` selon vos préférences.)

Exemples d'exécution
- Ethnicity (Excel) — crée/replace la table puis insère toutes les lignes:
```
python ingestion_ethnicity_data.py --pg-user admin --pg-pass admin \
	--pg-host localhost --pg-port 5434 --pg-db us_violent_incidents \
	--schema bronze --table ethnicity --source datasets/Ethnicity_Data_Usa.xlsx
```

- Cities (CSV chunked):
```
python ingestion_cities_data.py --pg-user admin --pg-pass admin \
	--pg-host localhost --pg-port 5434 --pg-db us_violent_incidents \
	--schema bronze --table cities --source datasets/uscities.csv --chunksize 50000
```

- Shootings (CSV chunked):
```
python ingestion_shootings_data.py --pg-user admin --pg-pass admin \
	--pg-host localhost --pg-port 5434 --pg-db us_violent_incidents \
	--schema bronze --table shootings --source datasets/fatal-police-shootings-data.csv --chunksize 20000
```

Conseils opérationnels & next steps
- Vérifier la connexion à la base et les droits sur le schéma ciblé avant d'exécuter.
- Si vous voulez préserver l'existant au lieu de `replace`, appeler les scripts
	avec `--if-exists append` (attention: la table doit alors déjà contenir les
	colonnes d'ingestion sinon l'append échouera).
- Ajouter les dépendances dans `pyproject.toml` ou créer un `requirements.txt`.
- Optionnel: ajouter un test rapide ou une commande `make test` pour valider
	la connectivité avant ingestion.

Contact & historique
- Ce README a été généré automatiquement après les modifications réalisées
	aux notebooks et la création des scripts d'ingestion.

---
Pour toute action de validation (exécuter un script contre la BD, créer les
fichiers de dépendances, ou pousser les changements), dites-moi ce que vous
voulez que j'exécute ensuite.

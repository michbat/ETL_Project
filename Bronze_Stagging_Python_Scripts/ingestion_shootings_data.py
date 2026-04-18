#!/usr/bin/env python
# coding: utf-8

"""
Ingestion pour fatal-police-shootings-data.csv -> PostgreSQL (chunked)

Le script lit le CSV par chunks, normalise les colonnes, force quelques types,
ajoute les colonnes d'ingestion (`source_filename`, `batch_id`, `load_datetime`)
et écrit les données vers PostgreSQL en créant la table si nécessaire.
"""

import os
import uuid
from datetime import datetime
import click
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import types as sqltypes
from tqdm.auto import tqdm

# Query string de connexion (format SQLAlchemy)
URL: str = "postgresql+psycopg://{user}:{pw}@{host}:{port}/{db}"


# Fonction pour normaliser les noms de colonnes (enlever les espaces, mettre en minuscules, etc.)
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("[^0-9a-z_]", "", regex=True)
    )
    return df

# Fonction pour forcer les types de données des colonnes pour éviter les erreurs d'ingestion
def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    # Dictionnaire de types de données souhaités pour les colonnes  (type de données SQLAlchemy)
    new_data_type = {
        'id': 'Int64',
        'name': 'string',
        'manner_of_death': 'string',
        'armed': 'string',
        'age': 'Int64',
        'gender': 'string',
        'race': 'string',
        'city': 'string',
        'state': 'string',
        'signs_of_mental_illness': 'boolean',
        'threat_level': 'string',
        'flee': 'string',
        'body_camera': 'boolean',
        'longitude': 'float64',
        'latitude': 'float64',
        'is_geocoding_exact': 'boolean'
    }
    
    # Forcer les types de données pour les colonnes présentes dans le DataFrame en itérant sur le dictionnaire et en transformant les types des  colonnes 
    for col, tp in new_data_type.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(tp)  # type: ignore
            except Exception:
                if tp.startswith("Int"):
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                else:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# Fonction pour ajouter les colonnes supplémentaires (source_filename, batch_id, load_datetime)
def add_ingestion_metadata(df: pd.DataFrame, source_path: str) -> pd.DataFrame:
    source_file = os.path.basename(source_path)
    batch_id = str(uuid.uuid4())
    load_dt = datetime.now()

    df["source_filename"] = source_file
    df["batch_id"] = batch_id
    df["load_datetime"] = load_dt

    df = df.astype({"source_filename": "string", "batch_id": "string", "load_datetime": "datetime64[ns]"})
    return df


# CLI avec Click pour utiliser des arguments optionnels flexibles lors de l'exécution du script en ligne de commande
@click.command()
@click.option("--pg-user", default="admin", help="Postgres user")
@click.option("--pg-pass", default="admin", help="Postgres password")
@click.option("--pg-host", default="localhost", help="Postgres host")
@click.option("--pg-port", default=5434, help="Postgres port", type=int)
@click.option("--pg-db", default="us_violent_incidents", help="Postgres database")
@click.option("--schema", default="bronze", help="Schema cible (ex: bronze)")
@click.option("--source", default="../datasets/fatal-police-shootings-data.csv", help="Chemin vers le fichier source CSV")
@click.option("--table", default="shootings", help="Nom de la table cible (sans schema) ou schema.table)")
@click.option("--chunksize", default=20000, type=int, help="Taille des chunks (nombre de lignes par batch)")
@click.option("--if-exists", default="replace", type=click.Choice(["replace", "append"]), help="Comportement si la table existe")

# Main function pour l'ingestion
def main(pg_user: str, pg_pass: str, pg_host: str, pg_port: int, pg_db: str, schema: str, source: str, table: str, chunksize: int, if_exists: str) -> None:
    """Ingère le CSV des shootings en mode chunked.
    """
    engine = create_engine(URL.format(user=pg_user, pw=pg_pass, host=pg_host, port=pg_port, db=pg_db))

    # Spliter le nom de la table si jamais précisé (ex: "bronze.cities" -> schema="bronze", table_only="cities")
    if "." in table:
        schema, table_only = table.split(".", 1)
    else:
        table_only = table

    # Définition des types de données pour les colonnes ajoutées lors de l'ingestion (pour la création de table)
    ingestion_dtype = {
        "source_filename": sqltypes.VARCHAR(length=255),
        "batch_id": sqltypes.VARCHAR(length=255),
        "load_datetime": sqltypes.TIMESTAMP(),
    }

    # Affichage du message de démarrage avec la source de données et la taille des chunks
    print(f"Lecture par chunks depuis: {source} (chunksize={chunksize})")
    new_parse_dates = ["date"] # On s'assure que le colonne "date" va être parsée en datetime dès la lecture du CSV pour éviter les problèmes d'ingestion vers PostgreSQL (si la colonne "date" est présente dans le CSV)

    print(f"Lecture par chunks depuis: {source} (chunksize={chunksize})")
    df_iter = pd.read_csv(source, iterator=True, chunksize=chunksize, low_memory=False, parse_dates=new_parse_dates)

    # S'assurer que le schema existe avant d'ingérer les données
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    first = True  # variable drapeau pour créer la table vie avant d'insérer les données
    total = 0  # variable pour compter le nombre total de lignes ingérées
    
    # Itération sur l'itérateur de DataFrames pour traiter et ingérer les données par chunks
    for df_chunk in tqdm(df_iter, desc="ingesting"):
        df_chunk = normalize_columns(df_chunk)  # enlèver les espaces, mettre en minuscules, etc.
        df_chunk = coerce_types(df_chunk)  # forcer les types des colonnes pour éviter les erreurs d'ingestion
        df_chunk = add_ingestion_metadata(df_chunk, source)  # ajout de colonnes supplémnetaires d'ingestion (source_filename, batch_id, load_datetime)

        if first:
            # Création de la structure de la table lors de la première itération avec remplacement de la table si elle existe déjà (option `if_exists="replace"`)
            df_chunk.head(0).to_sql(name=table_only, schema=schema, con=engine, if_exists="replace", index=False, dtype=ingestion_dtype)  # type: ignore
            first = False # on désactive le drapeau après la première itération pour les suivantes
            
        # Après la 1ère itération, on ajoute les données avec `if_exists="append"` pour ne pas écraser la table déjà créée
        df_chunk.to_sql(name=table_only, schema=schema, con=engine, if_exists="append", index=False)
        total += len(df_chunk) # on ajoute le nombre de lignes du chunk au total pour le message final

    print(f"Ingestion terminée: {total} lignes insérées dans {schema}.{table_only}.")


if __name__ == "__main__":
    main()  # type: ignore

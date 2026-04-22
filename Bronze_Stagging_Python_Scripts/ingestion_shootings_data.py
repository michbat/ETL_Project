#!/usr/bin/env python
# coding: utf-8

"""
Ingestion pour fatal-police-shootings-data.csv -> PostgreSQL (chunked)

Le script lit le CSV par chunks, normalise les colonnes, 
ajoute les colonnes d'ingestion (`source_filename`, `batch_id`, `load_datetime`)
et écrit les données vers PostgreSQL en créant la table si nécessaire.
"""

import os
import uuid
from datetime import datetime
import click
import pandas as pd
from sqlalchemy import create_engine, text

from tqdm.auto import tqdm

# Query string de connexion (format SQLAlchemy)
URL: str = "postgresql+psycopg://{user}:{pw}@{host}:{port}/{db}"

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise les noms de colonnes d'un DataFrame en enlevant les espaces, mettant en minuscules, etc."""
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("[^0-9a-z_]", "", regex=True)
    )
    return df

def add_ingestion_metadata(df: pd.DataFrame, source_path: str) -> pd.DataFrame:
    """Ajoute les colonnes d'ingestion (source_filename, batch_id, load_datetime) à un DataFrame."""
    source_file = os.path.basename(source_path)
    batch_id = str(uuid.uuid4())
    load_dt = datetime.now()

    df["source_filename"] = source_file
    df["batch_id"] = batch_id
    df["load_datetime"] = load_dt

    return df

# CLI avec Click pour utiliser des arguments optionnels flexibles lors de l'exécution du script # # en ligne de commande

@click.command()
@click.option("--pg-user", default="admin", help="Postgres user")
@click.option("--pg-pass", default="admin", help="Postgres password")
@click.option("--pg-host", default="localhost", help="Postgres host")
@click.option("--pg-port", default=5434, help="Postgres port", type=int)
@click.option("--pg-db", default="us_violent_incidents", help="Postgres database")
@click.option("--schema", default="bronze", help="Schema cible (ex: bronze)")
@click.option("--source", default="../datasets/fatal-police-shootings-data.csv", help="Chemin vers le fichier source CSV")
@click.option("--table", default="shootings_raw", help="Nom de la table cible (sans schema) ou schema.table)")
@click.option("--chunksize", default=20000, type=int, help="Taille des chunks (nombre de lignes par batch)")
@click.option("--if-exists", default="replace", type=click.Choice(["replace", "append"]), help="Comportement si la table existe")


def main(pg_user: str, pg_pass: str, pg_host: str, pg_port: int, pg_db: str, schema: str, source: str, table: str, chunksize: int, if_exists: str) -> None:
    """Fonction main orchestrant l'ingestion des données de la table shootings_raw."""
    # Création de l'engine SQLAlchemy pour la connexion à PostgreSQL
    engine = create_engine(URL.format(user=pg_user, pw=pg_pass, host=pg_host, port=pg_port, db=pg_db))

    # Spliter le nom de la table si jamais précisé (ex: "bronze.cities" -> schema="bronze", table_only="cities")
    if "." in table:
        schema, table_only = table.split(".", 1)
    else:
        table_only = table

    # Affichage du message de démarrage avec la source de données et la taille des chunks
    print(f"Lecture par chunks depuis: {source} (chunksize={chunksize})")
    
    # Définition de l'itérateur de DataFrames pour lire le CSV par chunks
    df_iter = pd.read_csv(source, iterator=True, chunksize=chunksize, low_memory=False)

    # S'assurer que le schema existe avant d'ingérer les données
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    first: bool = True  # variable drapeau pour créer la table avant d'insérer les données
    total: int = 0  # variable pour compter le nombre total de lignes ingérées
    
    # Itération sur l'itérateur de DataFrames pour traiter et ingérer les données par chunks
    for df_chunk in tqdm(df_iter, desc="ingesting"):
        df_chunk = normalize_columns(df_chunk)  # enlèver les espaces, mettre en minuscules, etc.
        df_chunk = add_ingestion_metadata(df_chunk, source)  # ajout de colonnes supplémentaires d'ingestion (source_filename, batch_id, load_datetime)

        if first:
            # Création de la structure de la table lors de la première itération avec remplacement de la table si elle existe déjà (option `if_exists="replace"`)
            df_chunk.head(0).to_sql(name=table_only, schema=schema, con=engine, if_exists="replace", index=False) 
            first = False # on met le drapeau à False après la première itération.
            
        # Après la 1ère itération, on ajoute les données avec `if_exists="append"` pour ne pas écraser la table déjà créée
        df_chunk.to_sql(name=table_only, schema=schema, con=engine, if_exists="append", index=False)
        total += len(df_chunk) # on ajoute le nombre de lignes ingérées dans le chunk au total

    print(f"Ingestion terminée: {total} lignes insérées dans {schema}.{table_only}.")


if __name__ == "__main__":
    main()  

#!/usr/bin/env python
# coding: utf-8

"""
Ingestion simple pour Ethnicity_Data_Usa.xlsx -> PostgreSQL

Le script lit le fichier Excel, normalise les colonnes,
ajoute les colonnes d'ingestion (`source_filename`, `batch_id`, `load_datetime`)
et écrit les données vers PostgreSQL en créant la table si nécessaire.
"""

import os
import uuid
from datetime import datetime
import click
import pandas as pd
from sqlalchemy import create_engine, text

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
@click.option("--source", default="../datasets/Ethnicity_Data_Usa.xlsx", help="Chemin vers le fichier source")
@click.option("--table", default="ethnicity_raw", help="Nom de la table cible (sans schema) ou schema.table)")
@click.option("--if-exists", default="replace", type=click.Choice(["replace", "append"]), help="Comportement si la table existe")


def main(pg_user: str, pg_pass: str, pg_host: str, pg_port: int, pg_db: str, schema: str, source: str, table: str, if_exists: str) -> None:
    """Fonction main orchestrant l'ingestion des données de la table ethnicity_raw."""
    # Création de l'engine SQLAlchemy pour la connexion à PostgreSQL
    engine = create_engine(URL.format(user=pg_user, pw=pg_pass, host=pg_host, port=pg_port, db=pg_db))

    # Lecture du fichier source (Excel)
    print(f"Lecture du fichier source: {source}")
    df = pd.read_excel(source)

    # Normalisation uniquement des colonnes (enlever les espaces, mettre en minuscules, etc.)
    df2 = normalize_columns(df)

    # Ajout des métadonnées d'ingestion
    df2 = add_ingestion_metadata(df2, source)

    # Détermination du schéma et du nom de la table si jamais précisé au format "schema.table"
    if "." in table:
        schema, table_only = table.split(".", 1)
    else:
        table_only = table

    # Création du schéma s'il n'existe pas déjà
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    print(f"Ecriture vers la table {table} (if_exists={if_exists})")
    
    # Création de la structure de la table lors de la première itération avec remplacement de la # table si elle existe déjà (option `if_exists="replace"`)
    df2.head(0).to_sql(name=table_only, schema=schema, con=engine, if_exists="replace", index=False) # type: ignore

    # Ensuite, on insère les données avec l'option `if_exists="append"` pour ajouter les données à la table créée précédemment
    df2.to_sql(name=table_only, schema=schema, con=engine, if_exists="append", index=False)

    print(f"Ingestion terminée: {len(df2)} lignes insérées dans {schema}.{table_only}.")


if __name__ == "__main__":
    main() 

#!/usr/bin/env python
# coding: utf-8

"""
Ingestion simple pour Ethnicity_Data_Usa.xlsx -> PostgreSQL


Le script lit le fichier Excel, normalise les colonnes, force quelques types,
ajoute les colonnes d'ingestion (`source_filename`, `batch_id`, `load_datetime`)
et écrit le résultat dans PostgreSQL en créant la table si nécessaire.
"""

import os
import uuid
from datetime import datetime
import click
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import types as sqltypes

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
    mapping = {
        "state": "string",
        "total_population": "Int64",
        "white": "Int64",
        "black": "Int64",
        "hispanic": "Int64",
        "asian": "Int64",
        "american_indian": "Int64",
    }
    
    # Forcer les types de données pour les colonnes présentes dans le DataFrame en itérant sur le dictionnaire et en transformant les types des  colonnes selon le mapping défini (les colonnes non présentes dans le DataFrame seront ignorées)
    for col, tp in mapping.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(tp) # type: ignore
            except Exception:
                # best-effort: coerce numeric when possible
                if tp.startswith("Int"):
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                else:
                    df[col] = df[col].astype("string")
    return df

# Fonction pour ajouter les colonnes supplémentaires (source_filename, batch_id, load_datetime)
def add_ingestion_metadata(df: pd.DataFrame, source_path: str) -> pd.DataFrame:
    source_file = os.path.basename(source_path)
    batch_id = str(uuid.uuid4())
    load_dt = datetime.now()

    df["source_filename"] = source_file
    df["batch_id"] = batch_id
    df["load_datetime"] = load_dt

    # ensure strings for the two varchar columns
    df = df.astype({"source_filename": "string", "batch_id": "string"})
    return df

# CLI avec Click pour utiliser des arguments optionnels flexibles lors de l'exécution du script en ligne de commande
@click.command()
@click.option("--pg-user", default="admin", help="Postgres user")
@click.option("--pg-pass", default="admin", help="Postgres password")
@click.option("--pg-host", default="localhost", help="Postgres host")
@click.option("--pg-port", default=5434, help="Postgres port", type=int)
@click.option("--pg-db", default="us_violent_incidents", help="Postgres database")
@click.option("--schema", default="bronze", help="Schema cible (ex: bronze)")
@click.option("--source", default="../datasets/Ethnicity_Data_Usa.xlsx", help="Chemin vers le fichier source")
@click.option("--table", default="ethnicity", help="Nom de la table cible (sans schema) ou schema.table)")
@click.option("--if-exists", default="replace", type=click.Choice(["replace", "append"]), help="Comportement si la table existe")


# Main function pour l'ingestion
def main(pg_user: str, pg_pass: str, pg_host: str, pg_port: int, pg_db: str, schema: str, source: str, table: str, if_exists: str) -> None:
    """Main ingestion routine."""
    # Build engine
    engine = create_engine(URL.format(user=pg_user, pw=pg_pass, host=pg_host, port=pg_port, db=pg_db))

    # Lecture du fichier source (Excel)
    print(f"Lecture du fichier source: {source}")
    df = pd.read_excel(source)

    # Normalisation et coercition des types
    df2 = normalize_columns(df)
    df2 = coerce_types(df2)

    # Ajout des métadonnées d'ingestion
    df2 = add_ingestion_metadata(df2, source)

    # Détermination du schéma et du nom de la table : permettre --table sous la forme 'schema.table' ou nom de table simple avec --schema
    if "." in table:
        schema, table_only = table.split(".", 1)
    else:
        table_only = table

    # Création du schéma si nécessaire
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    # Définition des types de données pour les colonnes d'ingestion (pour la création de table)
    dtype = {
        "source_filename": sqltypes.VARCHAR(length=255),
        "batch_id": sqltypes.VARCHAR(length=255),
        "load_datetime": sqltypes.TIMESTAMP(),
    }

    # Affichage du message de démarrage avec la source de données et la taille des chunks
    print(f"Ecriture vers la table {table} (if_exists={if_exists})")
    
    # Création de la structure de la table lors de la première itération avec remplacement de la table si elle existe déjà (option `if_exists="replace"`)
    df2.head(0).to_sql(name=table_only, schema=schema, con=engine, if_exists="replace", index=False, dtype=dtype) # type: ignore

    # Ensuite, on insère les données avec l'option `if_exists="append"` pour ajouter les données à la table créée précédemment
    df2.to_sql(name=table_only, schema=schema, con=engine, if_exists="append", index=False)
   
    # Message de fin d'ingestion avec le nombre de lignes insérées et le nom de la table cible
    print(f"Ingestion terminée: {len(df2)} lignes insérées dans {table}.")


if __name__ == "__main__":
    main()  # type: ignore

#!/usr/bin/env python
# coding: utf-8

"""
Ingester simple pour Ethnicity_Data_Usa.xlsx -> PostgreSQL

Usage:
    python ingestion_ethnicity_data.py --pg-user admin --pg-pass admin \
        --pg-host localhost --pg-port 5434 --pg-db us_violent_incidents \
        --source ../datasets/Ethnicity_Data_Usa.xlsx --table bronze.ethnicity

Le script lit le fichier Excel, normalise les colonnes, force quelques types,
ajoute les colonnes d'ingestion (`source_filename`, `batch_id`, `load_datetime`)
et écrit le résultat dans PostgreSQL en créant la table si nécessaire.
"""

# from __future__ import annotations

import os
import uuid
from datetime import datetime
import click
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import types as sqltypes


URL: str = "postgresql+psycopg://{user}:{pw}@{host}:{port}/{db}"


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("[^0-9a-z_]", "", regex=True)
    )
    return df


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    # Mapping minimal from notebook (nullable integers)
    mapping = {
        "state": "string",
        "total_population": "Int64",
        "white": "Int64",
        "black": "Int64",
        "hispanic": "Int64",
        "asian": "Int64",
        "american_indian": "Int64",
    }
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

def main(pg_user: str, pg_pass: str, pg_host: str, pg_port: int, pg_db: str, schema: str, source: str, table: str, if_exists: str) -> None:
    """Main ingestion routine."""
    # Build engine
    engine = create_engine(URL.format(user=pg_user, pw=pg_pass, host=pg_host, port=pg_port, db=pg_db))

    # Read source
    print(f"Lecture du fichier source: {source}")
    df = pd.read_excel(source)

    # Normalize and coerce
    df2 = normalize_columns(df)
    df2 = coerce_types(df2)

    # Add ingestion metadata
    df2 = add_ingestion_metadata(df2, source)

    # Determine schema and table name: allow --table as 'schema.table' or plain table name with --schema
    if "." in table:
        schema, table_only = table.split(".", 1)
    else:
        table_only = table

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    # dtype mapping for ingestion columns
    dtype = {
        "source_filename": sqltypes.VARCHAR(length=255),
        "batch_id": sqltypes.VARCHAR(length=255),
        "load_datetime": sqltypes.TIMESTAMP(),
    }

    # When creating the table, use df2.head(0) so ingestion columns are part of the schema
    print(f"Ecriture vers la table {table} (if_exists={if_exists})")
    df2.head(0).to_sql(name=table_only, schema=schema, con=engine, if_exists="replace", index=False, dtype=dtype) # type: ignore

    if if_exists == "replace":
        # replace already happened; insert all rows
        df2.to_sql(name=table_only, schema=schema, con=engine, if_exists="append", index=False)
    else:
        # append: just append
        df2.to_sql(name=table_only, schema=schema, con=engine, if_exists="append", index=False)

    print(f"Ingestion terminée: {len(df2)} lignes insérées dans {table}.")


if __name__ == "__main__":
    main()  # type: ignore

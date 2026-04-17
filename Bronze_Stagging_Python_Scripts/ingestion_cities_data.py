#!/usr/bin/env python
# coding: utf-8

"""
Ingester pour uscities.csv -> PostgreSQL (chunked)

Usage:
    python ingestion_cities_data.py --pg-user admin --pg-pass admin \
        --pg-host localhost --pg-port 5434 --pg-db us_violent_incidents \
        --source datasets/uscities.csv --schema bronze --table cities

Le script lit le CSV par chunks, normalise les colonnes, force quelques types,
ajoute les colonnes d'ingestion (`source_filename`, `batch_id`, `load_datetime`)
et écrit les données vers PostgreSQL en créant la table si nécessaire.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime
import click
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import types as sqltypes
from tqdm.auto import tqdm

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
    # Minimal coercions for common uscities columns (best-effort)
    mapping = {
        "population": "Int64",
        "density": "float64",
        "lat": "float64",
        "lng": "float64",
    }
    for col, tp in mapping.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(tp)  # type: ignore
            except Exception:
                if tp.startswith("Int"):
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                else:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def add_ingestion_metadata(df: pd.DataFrame, source_path: str) -> pd.DataFrame:
    source_file = os.path.basename(source_path)
    batch_id = str(uuid.uuid4())
    load_dt = datetime.now()

    df["source_filename"] = source_file
    df["batch_id"] = batch_id
    df["load_datetime"] = load_dt

    df = df.astype({"source_filename": "string", "batch_id": "string"})
    return df


@click.command()
@click.option("--pg-user", default="admin", help="Postgres user")
@click.option("--pg-pass", default="admin", help="Postgres password")
@click.option("--pg-host", default="localhost", help="Postgres host")
@click.option("--pg-port", default=5434, help="Postgres port", type=int)
@click.option("--pg-db", default="us_violent_incidents", help="Postgres database")
@click.option("--schema", default="bronze", help="Schema cible (ex: bronze)")
@click.option("--source", default="datasets/uscities.csv", help="Chemin vers le fichier source CSV")
@click.option("--table", default="cities", help="Nom de la table cible (sans schema) ou schema.table)")
@click.option("--chunksize", default=50000, type=int, help="Taille des chunks (nombre de lignes par batch)")
@click.option("--if-exists", default="replace", type=click.Choice(["replace", "append"]), help="Comportement si la table existe")
def main(pg_user: str, pg_pass: str, pg_host: str, pg_port: int, pg_db: str, schema: str, source: str, table: str, chunksize: int, if_exists: str) -> None:
    """Ingère le CSV des villes en mode chunked.
    """
    engine = create_engine(URL.format(user=pg_user, pw=pg_pass, host=pg_host, port=pg_port, db=pg_db))

    # Determine schema and table name: allow --table as 'schema.table' or plain table name with --schema
    if "." in table:
        schema, table_only = table.split(".", 1)
    else:
        table_only = table

    # dtype mapping is best-effort; avoid strict typing for unknown columns
    # ingestion columns SQL types
    ingestion_dtype = {
        "source_filename": sqltypes.VARCHAR(length=255),
        "batch_id": sqltypes.VARCHAR(length=255),
        "load_datetime": sqltypes.TIMESTAMP(),
    }

    # Read CSV iterator
    print(f"Lecture par chunks depuis: {source} (chunksize={chunksize})")
    df_iter = pd.read_csv(source, iterator=True, chunksize=chunksize, low_memory=False)

    # Ensure schema exists
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    first = True
    total = 0
    for df_chunk in tqdm(df_iter, desc="ingesting"):
        # normalize & coerce
        df_chunk = normalize_columns(df_chunk)
        df_chunk = coerce_types(df_chunk)

        # add ingestion metadata per chunk
        df_chunk = add_ingestion_metadata(df_chunk, source)

        if first:
            # create table structure from the DataFrame that includes ingestion columns
            df_chunk.head(0).to_sql(name=table_only, schema=schema, con=engine, if_exists="replace", index=False, dtype=ingestion_dtype)  # type: ignore
            first = False

        df_chunk.to_sql(name=table_only, schema=schema, con=engine, if_exists="append", index=False)
        total += len(df_chunk)

    print(f"Ingestion terminée: {total} lignes insérées dans {schema}.{table_only}.")


if __name__ == "__main__":
    main()  # type: ignore

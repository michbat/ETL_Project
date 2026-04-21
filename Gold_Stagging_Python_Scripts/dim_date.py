import pandas as pd
import time
import os
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.types import Integer, String, Date, SmallInteger
from typing import Dict

def get_engine() -> Engine:
    """Créer et retourne une instance de SQLAlchemy Engine pour se connecter à la base de données PostgreSQL."""
    # query_string = 'postgresql+psycopg://admin:admin@localhost:5434/us_violent_incidents'
    
    host = os.environ.get('POSTGRES_HOST', 'postgres-db')
    port = os.environ.get('POSTGRES_PORT', '5432')
    user = os.environ['POSTGRES_USER']
    password = os.environ['POSTGRES_PASSWORD']
    db = os.environ['POSTGRES_DB']
    query_string = f'postgresql+psycopg://{user}:{password}@{host}:{port}/{db}'
    return create_engine(query_string)

def load_data(engine: Engine) -> pd.DataFrame:
    """Charger les données de la table silver.shootings_enriched dans un DataFrame."""
    df = pd.read_sql_table('shootings_enriched', con=engine, schema='silver')
    return df

def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """Construire le DataFrame de la dimension date à partir de shootings_enriched."""
    df['date'] = pd.to_datetime(df['date'])
    df_dim_date = (
        df[['date']]
        .drop_duplicates()
        .sort_values('date')
        .reset_index(drop=True)
        .rename(columns={'date': 'full_date'})
    )
    df_dim_date['date_key'] = range(1, len(df_dim_date) + 1)
    df_dim_date['day'] = df_dim_date['full_date'].dt.day
    df_dim_date['month'] = df_dim_date['full_date'].dt.month
    df_dim_date['year'] = df_dim_date['full_date'].dt.year
    df_dim_date['quarter'] = df_dim_date['full_date'].dt.quarter
    df_dim_date['week_of_year'] = df_dim_date['full_date'].dt.isocalendar().week
    df_dim_date['week_name'] = df_dim_date['full_date'].dt.day_name()
    df_dim_date['is_weekend'] = df_dim_date['full_date'].dt.weekday.isin([5, 6]).astype(int)
    columns = ['date_key', 'full_date', 'day', 'month', 'year', 'quarter', 'week_of_year', 'week_name', 'is_weekend']
    return df_dim_date[columns]

def get_dtype_dict() -> Dict:
    """Retourner un dictionnaire de types pour la table dim_date."""
    return {
        'date_key': Integer(),
        'full_date': Date(),
        'day': SmallInteger(),
        'month': SmallInteger(),
        'year': Integer(),
        'quarter': SmallInteger(),
        'week_of_year': SmallInteger(),
        'week_name': String(20),
        'is_weekend': SmallInteger()
    }

def save_to_db(df: pd.DataFrame, engine: Engine, dtype_dict: Dict) -> None:
    """Enregistrer le DataFrame dim_date dans la table gold.dim_date de PostgreSQL."""
    # Créer le schema 'gold' s'il n'existe pas déjà
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
        
    # Enregistrer les données dans la table gold.dim_date en utilisant des chunks pour gérer les grandes quantités de données
    chunk_size = 500
    rows = 0
    start_time = time.time()
    for start in tqdm(range(0, len(df), chunk_size)):
        end = start + chunk_size
        df.iloc[start:end].to_sql(
            'dim_date',
            con=engine,
            schema='gold',
            if_exists='append' if start > 0 else 'replace',
            index=False,
            method='multi',
            dtype=dtype_dict
        )
        rows += len(df.iloc[start:end])
    elapsed_time = time.time() - start_time
    
    print(f"Toutes les données ont été écrites en {elapsed_time:.2f} secondes. {rows} lignes insérées.")
    
    # Ajouter la contrainte de clé primaire sur date_key après l'insertion des données
    with engine.begin() as conn:
        conn.execute(text("""
        ALTER TABLE gold.dim_date
        ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
    """))

def main() -> None:
    """Fonction main orchestrant le processus de construction et d'enregistrement de la dimension date."""
    engine = get_engine()
    df = load_data(engine)
    df_dim_date = build_dim_date(df)
    dtype_dict = get_dtype_dict()
    save_to_db(df_dim_date, engine, dtype_dict)

if __name__ == "__main__":
    main()

import pandas as pd
import time
import os
import calendar
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from typing import Dict
from sqlalchemy.types import Integer, String, SmallInteger

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

def load_dim_date(engine: Engine) -> pd.DataFrame:
    """Charger les données de la table gold.dim_date dans un DataFrame."""
    df = pd.read_sql_table('dim_date', con=engine, schema='gold')
    return df

def build_dim_month(df_dim_date: pd.DataFrame) -> pd.DataFrame:
    """Construire le DataFrame de la dimension mois à partir de dim_date."""
    df_dim_month = (
        df_dim_date[['year', 'month', 'quarter']]
        .drop_duplicates()
        .sort_values(['year', 'month'])
        .reset_index(drop=True)
    )
    df_dim_month['month_key'] = range(1, len(df_dim_month) + 1)
    df_dim_month = df_dim_month[['month_key', 'year', 'month', 'quarter']]
    df_dim_month['month_name'] = df_dim_month['month'].apply(lambda x: calendar.month_name[x])
    return df_dim_month

def get_dtype_dict() -> Dict:
    """Retourner un dictionnaire de types de données SQL compatibles pour la table dim_month."""
    return {
        'month_key': Integer(),
        'year': Integer(),
        'month': SmallInteger(),
        'quarter': SmallInteger(),
        'month_name': String(20)
    }

def save_to_db(df: pd.DataFrame, engine: Engine, dtype_dict: Dict) -> None:
    """Sauvegarder le DataFrame dans la table gold.dim_month en utilisant des chunks."""
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
    chunk_size: int = 500
    start_time: float = time.time()
    rows: int = 0
    for start in tqdm(range(0, len(df), chunk_size)):
        end: int = start + chunk_size
        df.iloc[start:end].to_sql(
            'dim_month',
            con=engine,
            schema='gold',
            if_exists='append' if start > 0 else 'replace',
            index=False,
            method='multi',
            chunksize=chunk_size,
            dtype=dtype_dict
        )
        rows += len(df.iloc[start:end])
    elapsed_time: float = time.time() - start_time
    print(f"Toutes les données ont été écrites en {elapsed_time:.2f} secondes. {rows} lignes insérées.")
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE gold.dim_month
            ADD CONSTRAINT pk_dim_month PRIMARY KEY (month_key)
        """))

def main() -> None:
    engine = get_engine()
    df_dim_date = load_dim_date(engine)
    df_dim_month = build_dim_month(df_dim_date)
    dtype_dict = get_dtype_dict()
    save_to_db(df_dim_month, engine, dtype_dict)

if __name__ == "__main__":
    main()

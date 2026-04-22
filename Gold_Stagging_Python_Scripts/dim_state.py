import pandas as pd
import time
import os
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from typing import Dict
from sqlalchemy.types import Integer, String, Float

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

def build_dim_state(df: pd.DataFrame) -> pd.DataFrame:
    """Construire le DataFrame de la dimension state à partir de shootings_enriched."""
    cols = [
        'state_code', 'state_name', 'total_population_state',
        'white_population', 'black_population', 'hispanic_population',
        'asian_population', 'american_indian_population',
        'pct_white', 'pct_black', 'pct_hispanic', 'pct_asian', 'pct_american_indian'
    ]
    df_dim_state = df[cols].drop_duplicates(subset=cols).reset_index(drop=True)
    df_dim_state['state_key'] = range(1, len(df_dim_state) + 1)
    df_dim_state = df_dim_state[
        ['state_key'] + cols
    ]
    return df_dim_state

def get_dtype_dict() -> Dict:
    """Retourner un dictionnaire de types de données SQL compatibles pour la table dim_state."""
    return {
        'state_key': Integer(),
        'state_code': String(),
        'state_name': String(),
        'total_population_state': Integer(),
        'white_population': Integer(),
        'black_population': Integer(),
        'hispanic_population': Integer(),
        'asian_population': Integer(),
        'american_indian_population': Integer(),
        'pct_white': Float(),
        'pct_black': Float(),
        'pct_hispanic': Float(),
        'pct_asian': Float(),
        'pct_american_indian': Float()
    }

def save_to_db(df: pd.DataFrame, engine: Engine, dtype_dict: Dict) -> None:
    """Sauvegarder le DataFrame dans la table gold.dim_state en utilisant des chunks."""
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
        
    chunk_size: int = 500
    start_time: float = time.time()
    rows: int = 0
    for start in tqdm(range(0, len(df), chunk_size)):
        end: int = start + chunk_size
        df.iloc[start:end].to_sql(
            'dim_state',
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
    print(f"Toutes les données ont été écrites en {elapsed_time:.2f} secondes. {rows} lignes ont été insérées dans la table 'gold.dim_state'")
    
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE gold.dim_state
            ADD CONSTRAINT pk_dim_state PRIMARY KEY (state_key)
        """))

def main() -> None:
    engine = get_engine()
    df_shootings = load_data(engine)
    df_dim_state = build_dim_state(df_shootings)
    dtype_dict = get_dtype_dict()
    save_to_db(df_dim_state, engine, dtype_dict)

if __name__ == "__main__":
    main()

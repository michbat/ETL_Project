import pandas as pd
import time
import os
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.types import Integer, String, SmallInteger
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

def build_dim_circumstance(df: pd.DataFrame) -> pd.DataFrame:
    """Construire le DataFrame de la dimension circumstance à partir de shootings_enriched."""
    
    # Colonnes à conserver pour la dimension circumstance
    cols = ['manner_of_death', 'threat_level', 'flee', 'body_camera_flag', 'is_geocoding_exact']
    df_dim_circumstance = df[cols].copy()
    
    # Supprimer les doublons afin d'avoir des enregistrements uniques pour chaque circumstance et réinitialiser les index
    df_dim_circumstance = df_dim_circumstance.drop_duplicates(subset=cols).reset_index(drop=True)
    
    # Ajouter une colonne circumstance_key en tant que clé primaire de la table dim_circumstance
    df_dim_circumstance['circumstance_key'] = range(1, len(df_dim_circumstance) + 1)
    
    # Réorganiser les colonnes pour que la clé primaire soit en premier
    df_dim_circumstance = df_dim_circumstance[
        ['circumstance_key', 'manner_of_death', 'threat_level', 'flee', 'body_camera_flag', 'is_geocoding_exact']
    ]
    return df_dim_circumstance

def get_dtype_dict() -> Dict:
    """Retourner un dictionnaire de types pour la table dim_circumstance."""
    return {
        'circumstance_key': Integer(),
        'manner_of_death': String(),
        'threat_level': String(),
        'flee': String(),
        'body_camera_flag': SmallInteger(),
        'is_geocoding_exact': SmallInteger()
    }

def save_to_db(df: pd.DataFrame, engine: Engine, dtype_dict: Dict) -> None:
    """Enregistrer le DataFrame dim_circumstance dans la table gold.dim_circumstance de PostgreSQL."""
    # Créer le schema gold s'il n'existe pas déjà
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
        
    # Utiliser la méthode d'insertion par chunks
    chunk_size = 50
    rows = 0
    start_time = time.time()
    for start in tqdm(range(0, len(df), chunk_size)):
        end = start + chunk_size
        df.iloc[start:end].to_sql(
            'dim_circumstance',
            con=engine,
            schema='gold',
            if_exists='append' if start > 0 else 'replace',
            index=False,
            method='multi',
            chunksize=chunk_size,
            dtype=dtype_dict
        )
        rows += len(df.iloc[start:end])
        
    elapsed_time = time.time() - start_time
    print(f"Toutes les données ont été écrites en {elapsed_time:.2f} secondes. {rows} lignes insérées.")
    
    # Ajouter la contrainte de clé primaire après l'insertion des données
    with engine.begin() as conn:
        conn.execute(text("""
        ALTER TABLE gold.dim_circumstance
        ADD CONSTRAINT pk_dim_circumstance PRIMARY KEY (circumstance_key)
        """))

def main() -> None:
    """Fonction main orchestrant le processus de création et d'enregistrement de la dimension circumstance."""
    engine = get_engine()
    df = load_data(engine)
    df_dim_circumstance = build_dim_circumstance(df)
    dtype_dict = get_dtype_dict()
    save_to_db(df_dim_circumstance, engine, dtype_dict)

if __name__ == "__main__":
    main()

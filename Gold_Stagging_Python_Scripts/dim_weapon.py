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

def build_dim_weapon(df: pd.DataFrame) -> pd.DataFrame:
    """Construire le DataFrame de la dimension weapon à partir de shootings_enriched."""
    
    # Colonnes à conserver pour la dimension weapon
    cols = ['armed', 'weapon_category', 'armed_flag', 'unarmed_flag']
    df_dim_weapon = df[cols].copy()
    
    # supprimer les doublons afin d'avoir des enregistrements uniques pour chaque combinaison de armed, weapon_category, armed_flag et unarmed_flag, et réinitialiser les index
    df_dim_weapon = df_dim_weapon.drop_duplicates(subset=cols).reset_index(drop=True)
    
    # Ajouter une colonne weapon_key en tant que clé primaire de la table dim_weapon
    df_dim_weapon['weapon_key'] = range(1, len(df_dim_weapon) + 1)
    
    # Ajouter des colonnes is_vehicle et is_toy_weapon basées sur les valeurs de flee et armed
    df_dim_weapon['is_vehicle'] = df['flee'].apply(lambda x: 1 if x == 'car' else 0)
    df_dim_weapon['is_toy_weapon'] = df['armed'].apply(lambda x: 1 if x == 'toy weapon' else 0)
    
    # Renommer les colonnes selon le cahier des charges et réorganiser les colonnes
    df_dim_weapon.rename(columns={
        'armed': 'armed_raw',
        'armed_flag': 'is_armed',
        'unarmed_flag': 'is_unarmed'
    }, inplace=True)
    df_dim_weapon = df_dim_weapon[[
        'weapon_key', 'armed_raw', 'weapon_category', 'is_armed', 'is_unarmed', 'is_vehicle', 'is_toy_weapon'
    ]]
    return df_dim_weapon

def get_dtype_dict() -> Dict:
    """Retourner un dictionnaire de types pour la table dim_weapon."""
    return {
        'weapon_key': Integer(),
        'armed_raw': String(),
        'weapon_category': String(),
        'is_armed': SmallInteger(),
        'is_unarmed': SmallInteger(),
        'is_vehicle': SmallInteger(),
        'is_toy_weapon': SmallInteger()
    }

def save_to_db(df: pd.DataFrame, engine: Engine, dtype_dict: Dict) -> None:
    """Enregistrer le DataFrame dim_weapon dans la table gold.dim_weapon de PostgreSQL."""
    # Créer le schema 'gold' s'il n'existe pas déjà
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
        
    # Insérer les données dans la table 'gold.dim_weapon' en utilisant les chunks
    chunk_size:int = 500
    rows:int = 0
    start_time: float = time.time()
    for start in tqdm(range(0, len(df), chunk_size)):
        end = start + chunk_size
        df.iloc[start:end].to_sql(
            'dim_weapon',
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
    
    # Ajouter la contrainte de clé primaire sur weapon_key après l'insertion des données
    with engine.begin() as conn:
        conn.execute(text("""
        ALTER TABLE gold.dim_weapon
        ADD CONSTRAINT pk_dim_weapon PRIMARY KEY (weapon_key)
        """))

def main() -> None:
    """Fonction main orchestrant le processus de création et d'enregistrement de la dimension weapon dans la table gold.dim_weapon."""
    engine = get_engine()
    df = load_data(engine)
    df_dim_weapon = build_dim_weapon(df)
    dtype_dict = get_dtype_dict()
    save_to_db(df_dim_weapon, engine, dtype_dict)

if __name__ == "__main__":
    main()

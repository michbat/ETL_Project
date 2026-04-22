import pandas as pd
import time
import os
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.types import Integer, String, Float
from typing import Dict


def get_engine() -> Engine:
    """ Crée et retourne une instance de SQLAlchemy Engine pour se connecter à la base de données PostgreSQL."""
    # query_string = 'postgresql+psycopg://admin:admin@localhost:5434/us_violent_incidents'
    
    host = os.environ.get('POSTGRES_HOST', 'postgres-db')
    port = os.environ.get('POSTGRES_PORT', '5432')
    user = os.environ['POSTGRES_USER']
    password = os.environ['POSTGRES_PASSWORD']
    db = os.environ['POSTGRES_DB']

    query_string = f'postgresql+psycopg://{user}:{password}@{host}:{port}/{db}'
    return create_engine(query_string)


def load_data(engine: Engine) -> pd.DataFrame:
    """ Charge les données de la table bronze.uscities_raw dans un DataFrame pandas."""
    df = pd.read_sql_table('uscities_raw', con=engine, schema='bronze')
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """ Effectue le nettoyage et les transformations nécessaires sur le DataFrame des villes."""
    
    # Garder uniquement les colonnes nécessaires pour l'analyse et la fusion avec les shootings
    columns_to_keep = ['city','state_id','state_name','county_name','lat','lng','population','density','timezone']
    df = df[columns_to_keep]
    
    # Supprimer les doublons basés sur la combinaison de city et state_id, en gardant la ligne avec la population la plus élevée
    df = df.sort_values('population', ascending=False).drop_duplicates(subset=['city', 'state_id'], keep='first')
    
    # Renommer les colonnes lat et lng en city_latitude et city_longitude pour éviter les conflits lors de la fusion avec les shootings
    df.rename(columns={'lat': 'city_latitude', 'lng': 'city_longitude'}, inplace=True)
    
    # Ajouter une colonne id_city basée sur l'index du DataFrame pour servir de clé primaire
    df.reset_index(drop=True, inplace=True)
    df['id_city'] = df.index + 1
    cols = df.columns.tolist()
    cols = ['id_city'] + [col for col in cols if col != 'id_city']
    df = df[cols]
    
    # Mettre les noms de ville, d'état et de comté en minuscules pour assurer la cohérence lors de la fusion avec les shootings   
    for col in ['city', 'state_id', 'state_name', 'county_name']:
        df[col] = df[col].str.lower()
        
    # Renommer la colonne state_id en state_code pour assurer la cohérence avec les shootings
    df.rename(columns={'state_id': 'state_code'}, inplace=True)
    
    # Supprimer les lignes où la population est nulle ou négative, car elles ne sont pas pertinentes pour l'analyse des shootings
    df = df[df['population'] > 0]
    
    # Retourner le DataFrame nettoyé et transformé
    return df


def get_dtype_dict() -> Dict:
    """ Retourne un dictionnaire de types de données pour les colonnes du DataFrame des villes, utilisé lors de l'écriture dans la base de données."""
    return {
        'id_city': Integer(),
        'city': String(100),
        'state_code': String(5),
        'state_name': String(100),
        'county_name': String(100),
        'city_latitude': Float(),
        'city_longitude': Float(),
        'population': Integer(),
        'density': Float(),
        'timezone': String(150)
    }


def save_to_db(df: pd.DataFrame, engine: Engine, dtype_dict: Dict) -> None:
    """ Enregistre le DataFrame des villes nettoyé et transformé dans la table silver.cities_clean de la base de données PostgreSQL."""
    
    # Créer le schéma silver s'il n'existe pas déjà
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        
    # Enregistrer les données dans la table silver.cities_clean en utilisant des chunks pour gérer les grandes quantités de données et éviter les problèmes de mémoire
    chunk_size: int = 5000
    rows: int = 0
    start_time: float = time.time()
    for start in tqdm(range(0, len(df), chunk_size)):
        end = start + chunk_size
        df.iloc[start:end].to_sql(
            'uscities_clean',
            con=engine,
            schema='silver',
            if_exists='append' if start > 0 else 'replace',
            index=False,
            method='multi',
            dtype=dtype_dict
        )
        rows += len(df.iloc[start:end])
    elapsed_time: float = time.time() - start_time
    print(f"Toutes les données ont été écrites en {elapsed_time:.2f} secondes. {rows} lignes ont été insérées dans la table 'silver.uscities_clean'")
    
    # Ajouter une clé primaire à la table silver.uscities_clean sur la colonne id_city
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE silver.uscities_clean
            ADD CONSTRAINT pk_uscities_clean PRIMARY KEY (id_city);
        """))

def main() -> None:
    """ Fonction main qui exécute les étapes de chargement, nettoyage et enregistrement des données des villes."""
    engine = get_engine()
    df = load_data(engine)
    df = clean_data(df)
    dtype_dict = get_dtype_dict()
    save_to_db(df, engine, dtype_dict)

if __name__ == "__main__":
    main()

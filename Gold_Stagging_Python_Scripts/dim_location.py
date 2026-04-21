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

def build_dim_location(df: pd.DataFrame) -> pd.DataFrame:
    """Construire le DataFrame de la dimension location à partir de shootings_enriched."""
    
    # Colonnes à conserver
    cols = [
        'city', 'state_code', 'state_name', 'county_name', 'timezone',
        'city_latitude', 'city_longitude', 'incident_latitude', 'incident_longitude',
        'population', 'density', 'total_population_state', 'pct_white', 'pct_black',
        'pct_hispanic', 'pct_asian', 'pct_american_indian'
    ]
    
    # Créer le dataframe df_dim_location en sélectionnant les colonnes nécessaires et en ajoutant une colonne location_description
    df_dim_location = df[cols].copy()
    
    # Supprimer les doublons afin d'avoir des enregistrements uniques pour chaque location et réinitialiser les index
    df_dim_location = df_dim_location.drop_duplicates(subset=cols).reset_index(drop=True)
    
    # Ajouter la colonne location_description en concaténant les informations de ville, comté et état
    df_dim_location['location_description'] = (
        df_dim_location['city'] + ', ' +
        df_dim_location['county_name'] + ', ' +
        df_dim_location['state_name']
    )
    
    # Ajouter une colonne location_key en tant que clé primaire de la table dim_location
    df_dim_location['location_key'] = range(1, len(df_dim_location) + 1)
    
    # Réorganiser les colonnes
    columns = [
        'location_key', 'city', 'state_code', 'state_name', 'county_name', 'timezone',
        'city_latitude', 'city_longitude', 'incident_latitude', 'incident_longitude',
        'population', 'density', 'total_population_state', 'pct_white', 'pct_black',
        'pct_hispanic', 'pct_asian', 'pct_american_indian', 'location_description'
    ]
    return df_dim_location[columns]

def get_dtype_dict() -> Dict:
    """Retourner un dictionnaire de types pour la table dim_location."""
    
    # Faire le mapping en vue  d'avoir les types de données SQL compatibles pour la table dim_location
    return {
        'location_key': Integer(),
        'city': String(100),
        'state_code': String(10),
        'state_name': String(100),
        'county_name': String(100),
        'timezone': String(50),
        'city_latitude': Float(),
        'city_longitude': Float(),
        'incident_latitude': Float(),
        'incident_longitude': Float(),
        'population': Integer(),
        'density': Float(),
        'total_population_state': Integer(),
        'pct_white': Float(),
        'pct_black': Float(),
        'pct_hispanic': Float(),
        'pct_asian': Float(),
        'pct_american_indian': Float(),
        'location_description': String(255)
    }

def save_to_db(df: pd.DataFrame, engine: Engine, dtype_dict: Dict) -> None:
    """Enregistrer le DataFrame dim_location dans la table gold.dim_location de PostgreSQL."""
    
    # Créer le schema 'gold' s'il n'existe pas déjà
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
        
    # Enregistrer les données dans la table gold.dim_location en utilisant des chunks pour gérer les grandes quantités de données
    chunk_size = 500
    rows = 0
    start_time = time.time()
    for start in tqdm(range(0, len(df), chunk_size)):
        end = start + chunk_size
        df.iloc[start:end].to_sql(
            'dim_location',
            con=engine,
            schema='gold',
            if_exists='append' if start > 0 else 'replace',
            index=False,
            method='multi',
            dtype=dtype_dict
        )
        rows += len(df.iloc[start:end])
    elapsed_time = time.time() - start_time 
    
    # Afficher le temps écoulé et le nombre de lignes insérées
    print(f"Toutes les données ont été écrites en {elapsed_time:.2f} secondes. {rows} lignes insérées.")
    
    # Ajouter la contrainte de clé primaire sur location_key après l'insertion des données
    with engine.begin() as conn:
        conn.execute(text("""
        ALTER TABLE gold.dim_location
        ADD CONSTRAINT pk_dim_location PRIMARY KEY (location_key)
        """))

def main() -> None:
    """Fonction main orchestrant le processus de construction et d'enregistrement de la dimension location."""
    engine = get_engine()
    df = load_data(engine)
    df_dim_location = build_dim_location(df)
    dtype_dict = get_dtype_dict()
    save_to_db(df_dim_location, engine, dtype_dict)

if __name__ == "__main__":
    main()

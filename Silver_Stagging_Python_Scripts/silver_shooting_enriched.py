import pandas as pd
import time
import os
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.types import Integer, String, Float, Date, SmallInteger, DateTime
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

def load_data(engine: Engine) -> Dict[str, pd.DataFrame]:
    """ Charge les tables depuis la base de données et retourne un dictionnaire de DataFrames. """
    df_shootings = pd.read_sql_query(text('SELECT * FROM silver.shootings_clean'), con=engine)
    df_cities = pd.read_sql_query(text('SELECT * FROM silver.uscities_clean'), con=engine)
    df_ethnicity = pd.read_sql_query(text('SELECT * FROM silver.ethnicity_clean'), con=engine)
    return {'shootings': df_shootings, 'cities': df_cities, 'ethnicity': df_ethnicity}

def categorize_weapon(weapon: str) -> str:
    """ Catégorise les armes en différentes catégories. """
    if weapon in ['gun', 'gun and knife', 'gun and vehicle']:
        return 'Firearm'
    elif weapon in ['knife', 'machete', 'ax', 'sword', 'hammer']:
        return 'Sharp/Blunt Weapon'
    elif weapon == 'toy weapon':
        return 'Toy Weapon'
    elif weapon == 'unarmed':
        return 'Unarmed'
    elif weapon in ['unknown weapon', 'undetermined', None]:
        return 'Unknown'
    else:
        return 'Other Weapon'

def enrich_data(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """ Enrichit les données en ajoutant des colonnes calculées et en fusionnant les tables. """
    
    # Récupérer les DataFrames du dictionnaire en utilisant des clés explicites
    df_shootings = dfs['shootings']
    df_cities = dfs['cities']
    df_ethnicity = dfs['ethnicity']
    
    # Fusionner le DataFrame des shootings avec celui des villes sur les colonnes 'city' et 'state_code'
    df = df_shootings.merge(df_cities, how='inner', on=['city','state_code'])
    
    # Fusionner le DataFrame résultant avec celui de l'ethnicité sur la colonne 'state_code'
    df = df.merge(df_ethnicity, how='inner', on='state_code')
    
    # Supprimer la colonne 'state_name_y' résultant de la fusion et renommer 'state_name_x' en 'state_name'
    df.drop(columns=['state_name_y'], inplace=True)
    df.rename(columns={'state_name_x': 'state_name'}, inplace=True)
    
    # Convertir la colonne 'date' en type datetime et extraire des informations temporelles
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['quarter'] = df['date'].dt.quarter
    df['weekday'] = df['date'].dt.weekday
    
    # Ajouter une colonne pour indiquer si le jour de la semaine est un week-end
    df['is_weekend'] = df['weekday'].apply(lambda x: 1 if x >= 5 else 0)
    
    # Créer des bandes d'âge à partir de la colonne 'age'
    bins = [0, 17, 24, 34, 44, 54, 64, float('inf')]
    labels = ['0-17', '18-24', '25-34', '35-44', '45-54', '55-64', '65+']
    df['age_band'] = pd.cut(df['age'], bins=bins, labels=labels, right=True, include_lowest=True)
    
    # Traiter les cas où l'âge est 0 en les assignant à la bande '0-17'
    df.loc[df['age'] == 0, 'age_band'] = '0-17'
    
    # Renommer la colonne 'race' en 'race_label' pour plus de clarté
    df = df.rename(columns={'race': 'race_label', 'total_population': 'total_population_state', 'body_camera': 'body_camera_flag'})
    
    # Ajouter des indicateurs binaires pour les colonnes 'flee' et 'armed'
    df['flee_flag'] = df['flee'].apply(lambda x: 0 if x == 'not fleeing' else 1)
    df['armed_flag'] = df['armed'].apply(lambda x: 0 if x == 'unarmed' else 1)
    df['unarmed_flag'] = df['armed'].apply(lambda x: 1 if x == 'unarmed' else 0)
    
    # Catégoriser les armes en utilisant la fonction categorize_weapon
    df['weapon_category'] = df['armed'].apply(categorize_weapon)
    
    # Ajouter une colonne 'race_code' en utilisant un mapping des labels de race
    race_code_mapping = {
        'white': 'W',
        'black': 'B',
        'hispanic': 'H',
        'asian': 'A',
        'native american': 'N',
        'other': 'O',
        'unknown': 'U'
    }

    # Appliquer le mapping des codes de race
    df['race_code'] = df['race_label'].map(race_code_mapping)
        
    # Retourner le DataFrame enrichi
    return df

def get_dtype_dict() -> Dict:
    """ Retourne un dictionnaire de types de données pour les colonnes du DataFrame enrichi, utilisé lors de l'écriture dans la base de données. """
    return {
        'id_shooting': Integer(),
        'name': String(255),
        'date': DateTime(),
        'manner_of_death': String(50),
        'armed': String(100),
        'age': SmallInteger(),
        'gender': String(20),
        'race_label': String(50),
        'city': String(100),
        'state_code': String(5),
        'signs_of_mental_illness': SmallInteger(),
        'threat_level': String(50),
        'flee': String(50),
        'body_camera': SmallInteger(),
        'incident_longitude': Float(),
        'incident_latitude': Float(),
        'is_geocoding_exact': SmallInteger(),
        'id_city': Integer(),
        'state_name': String(50),
        'county_name': String(100),
        'city_latitude': Float(),
        'city_longitude': Float(),
        'population': Integer(),
        'density': Float(),
        'timezone': String(50),
        'id_ethnicity': Integer(),
        'total_population_state': Integer(),
        'white': Integer(),
        'black': Integer(),
        'hispanic': Integer(),
        'asian': Integer(),
        'american_indian': Integer(),
        'pct_white': Float(),
        'pct_black': Float(),
        'pct_hispanic': Float(),
        'pct_asian': Float(),
        'pct_american_indian': Float(),
        'year': SmallInteger(),
        'month': SmallInteger(),
        'quarter': SmallInteger(),
        'weekday': SmallInteger(),
        'is_weekend': SmallInteger(),
        'age_band': String(10),
        'flee_flag': SmallInteger(),
        'armed_flag': SmallInteger(),
        'unarmed_flag': SmallInteger(),
        'weapon_category': String(50)
    }

def save_to_db(df: pd.DataFrame, engine: Engine, dtype_dict: Dict) -> None:
    """ Enregistre le DataFrame enrichi dans la table silver.shootings_enriched de la base de données PostgreSQL. """
    
    # Créer le schéma silver s'il n'existe pas déjà
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        
    # Enregistrer les données dans la table silver.shootings_enriched en utilisant des chunks pour gérer les grandes quantités de données
    chunk_size = 500
    rows = 0
    start_time = time.time()
    for start in tqdm(range(0, len(df), chunk_size)):
        end = start + chunk_size
        df.iloc[start:end].to_sql(
            'shootings_enriched',
            con=engine,
            schema='silver',
            if_exists='append' if start > 0 else 'replace',
            index=False,
            method='multi',
            chunksize=chunk_size,
            dtype=dtype_dict
        )
        rows += len(df.iloc[start:end])
    elapsed_time = time.time() - start_time
    print(f"Toutes les données ont été écrites en {elapsed_time:.2f} secondes. {rows} lignes insérées.")

def main() -> None:
    """Fonction main pour exécuter le processus de chargement, nettoyage, enrichissement et sauvegarde des données de shootings."""
    engine = get_engine()
    dfs = load_data(engine)
    df_enriched = enrich_data(dfs)
    dtype_dict = get_dtype_dict()
    save_to_db(df_enriched, engine, dtype_dict)

if __name__ == "__main__":
    main()

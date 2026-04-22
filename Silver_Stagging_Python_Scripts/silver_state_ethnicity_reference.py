import pandas as pd
import os
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.types import Integer, String, Float
from typing import Dict

def get_engine() -> Engine:
    """ Crée et retourne une connexion à la base de données PostgreSQL."""
    query_string = 'postgresql+psycopg://admin:admin@localhost:5434/us_violent_incidents'
    host = os.environ.get('POSTGRES_HOST', 'postgres-db')
    port = os.environ.get('POSTGRES_PORT', '5432')
    user = os.environ['POSTGRES_USER']
    password = os.environ['POSTGRES_PASSWORD']
    db = os.environ['POSTGRES_DB']

    query_string = f'postgresql+psycopg://{user}:{password}@{host}:{port}/{db}'
    return create_engine(query_string)

def load_data(engine: Engine) -> pd.DataFrame:
    """ Charge les données de la table bronze.ethnicity_raw dans un DataFrame pandas."""
    df = pd.read_sql_table('ethnicity_raw', con=engine, schema='bronze')
    return df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """ Effectue le nettoyage et les transformations nécessaires sur le DataFrame des ethnies."""
    
    # Supprimer les colonnes inutiles
    df = df.drop(columns=['source_filename', 'batch_id','load_datetime'])
    
    # Calculer les pourcentages pour chaque groupe ethnique
    df['pct_white'] = round((df['white'] / df['total_population']) * 100, 2)
    df['pct_black'] = round((df['black'] / df['total_population']) * 100, 2)
    df['pct_hispanic'] = round((df['hispanic'] / df['total_population']) * 100, 2)
    df['pct_asian'] = round((df['asian'] / df['total_population']) * 100, 2)
    df['pct_american_indian'] = round((df['american_indian'] / df['total_population']) * 100, 2)
    
    # Réinitialiser l'index pour créer une colonne d'identifiant qui sera utilisée comme clé primaire
    df.reset_index(drop=True, inplace=True)
    df['id_ethnicity'] = df.index + 1
    cols = df.columns.tolist()
    cols = ['id_ethnicity'] + [col for col in cols if col != 'id_ethnicity']
    df = df[cols]
    
    # Convertir les valeurs de la colonne state en minuscules et renommer la colonne en state_name
    df['state'] = df['state'].str.lower()
    df.rename(columns={'state': 'state_name'}, inplace=True)
    
    # Mapper les noms d'état à leurs codes correspondants
    state_code_mapping = {
        'alabama': 'al', 'alaska': 'ak', 'arizona': 'az', 'arkansas': 'ar', 'california': 'ca',
        'colorado': 'co', 'connecticut': 'ct', 'delaware': 'de', 'florida': 'fl', 'georgia': 'ga',
        'hawaii': 'hi', 'idaho': 'id', 'illinois': 'il', 'indiana': 'in', 'iowa': 'ia', 'kansas': 'ks',
        'kentucky': 'ky', 'louisiana': 'la', 'maine': 'me', 'maryland': 'md', 'massachusetts': 'ma',
        'michigan': 'mi', 'minnesota': 'mn', 'mississippi': 'ms', 'missouri': 'mo', 'montana': 'mt',
        'nebraska': 'ne', 'nevada': 'nv', 'new hampshire': 'nh', 'new jersey': 'nj', 'new mexico': 'nm',
        'new york': 'ny', 'north carolina': 'nc', 'north dakota': 'nd', 'ohio': 'oh', 'oklahoma': 'ok',
        'oregon': 'or', 'pennsylvania': 'pa', 'rhode island': 'ri', 'south carolina': 'sc',
        'south dakota': 'sd', 'tennessee': 'tn', 'texas': 'tx', 'utah': 'ut', 'vermont': 'vt',
        'virginia': 'va', 'washington': 'wa', 'west virginia': 'wv', 'wisconsin': 'wi', 'wyoming': 'wy',
        'puerto rico': 'pr', 'district of columbia': 'dc'
    }
    
    # Ajouter une colonne state_code en mappant les noms d'état à leurs codes correspondants
    df['state_code'] = df['state_name'].map(state_code_mapping)
    
    # Réorganiser les colonnes pour que state_code vienne après state_name
    cols = df.columns.tolist()
    state_name_index = cols.index('state_name')
    
    # cols.pop(cols.index('state_code')) supprime la colonne state_code de sa position actuelle et retourne sa valeur, puis cols.insert(state_name_index + 1, ...) insère cette valeur juste après state_name
    cols.insert(state_name_index + 1, cols.pop(cols.index('state_code')))
    
    df = df[cols] # Réorganiser les colonnes selon la nouvelle liste de colonnes
    
    # Retourner le DataFrame nettoyé et transformé
    return df

def get_dtype_dict() -> Dict:
    """ Définit et retourne un dictionnaire de types de données pour les colonnes du DataFrame des ethnies, utilisé lors de l'enregistrement dans la base de données."""
    return {
        'id_ethnicity': Integer(),
        'state_name': String(100),
        'state_code': String(5),
        'total_population': Integer(),
        'white': Integer(),
        'black': Integer(),
        'hispanic': Integer(),
        'asian': Integer(),
        'american_indian': Integer(),
        'pct_white': Float(),
        'pct_black': Float(),
        'pct_hispanic': Float(),
        'pct_asian': Float(),
        'pct_american_indian': Float()
    }

def save_to_db(df: pd.DataFrame, engine: Engine, dtype_dict: Dict) -> None:
    """ Enregistre le DataFrame des ethnies nettoyé et transformé dans la table silver.ethnicity_clean de la base de données PostgreSQL."""
    
    # Créer le schéma silver s'il n'existe pas déjà
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        
    # Enregistrer les données dans la table silver.ethnicity_clean
    df.to_sql('ethnicity_clean', con=engine, schema='silver', if_exists='replace', index=False, dtype=dtype_dict)
    print(f"{len(df)} lignes ont été insérées dans la table 'silver.ethnicity_clean'")
    
    # Ajouter une clé primaire à la table
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE silver.ethnicity_clean
            ADD CONSTRAINT pk_ethnicity_clean PRIMARY KEY (id_ethnicity);
        """))

def main() -> None:
    """ Fonction main qui orchestre le processus de chargement, nettoyage et enregistrement des données d'ethnie."""
    engine = get_engine()
    df = load_data(engine)
    df = clean_data(df)
    dtype_dict = get_dtype_dict()
    save_to_db(df, engine, dtype_dict)

if __name__ == "__main__":
    main()

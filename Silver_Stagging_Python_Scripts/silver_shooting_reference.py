import pandas as pd
import time
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.types import Integer, String, Date, Float, SmallInteger
from typing import Dict

def get_engine() -> Engine:
    """ Crée et retourne une instance de SQLAlchemy Engine pour se connecter à la base de données PostgreSQL."""
    query_string = 'postgresql+psycopg://admin:admin@localhost:5434/us_violent_incidents'
    return create_engine(query_string)

def load_data(engine: Engine) -> pd.DataFrame:
    """ Charge les données de la table bronze.shootings dans un DataFrame pandas."""
    df = pd.read_sql_table('shootings', con=engine, schema='bronze')
    return df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """ Effectue le nettoyage et les transformations nécessaires sur le DataFrame des shootings."""
    # Supprimer les colonnes inutiles
    df = df.drop(columns=['id','source_filename', 'batch_id', 'load_datetime'])
    
    # Convertir les colonnes booléennes en entiers (0 et 1)
    bool_columns = ['body_camera', 'is_geocoding_exact', 'signs_of_mental_illness']
    for col in bool_columns:
        df[col] = df[col].astype(bool).astype(int)
    
    # Nettoyer et normaliser les colonnes de texte
    text_columns = ['name', 'manner_of_death','armed','gender','race','city','state','threat_level','flee']
    for col in text_columns:
        df[col] = df[col].astype(str).str.strip().str.lower()
    
    # Remplacer les valeurs manquantes dans les colonnes de texte par 'Not specified'
    for col in ['name', 'race', 'flee', 'armed', 'gender']:
        df[col] = df[col].fillna('Not specified')
    
    # Remplacer les codes de genre et de race par des valeurs plus lisibles
    df['gender'] = df['gender'].replace({'m': 'male', 'f': 'female'})
    df['race'] = df['race'].replace({
        'w': 'white',
        'b': 'black',
        'a': 'asian',
        'h': 'hispanic',
        'n': 'native american',
        'o': 'other',
    })
    
    # Ajouter une colonne id_shooting basée sur l'index du DataFrame pour servir de clé primaire
    df.reset_index(drop=True, inplace=True)
    df['id_shooting'] = df.index + 1
    cols = df.columns.tolist()
    cols = ['id_shooting'] + [col for col in cols if col != 'id_shooting']
    df = df[cols]
    
    # Renommer la colonne state en state_code
    df.rename(columns={'state': 'state_code'}, inplace=True)
    
    # Nettoyer les valeurs d'âge et remplacer les valeurs manquantes ou négatives par 0
    df['age'] = df['age'].apply(lambda x: 0 if pd.isnull(x) or x < 0 else x)
    return df

def fill_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """ Remplit les coordonnées géographiques (latitude et longitude) des incidents de shootings en utilisant les données de la table silver.cities_clean."""
    
    # Créer une colonne de clé de ville pour faire la correspondance avec les coordonnées de la table des villes
    df['city_key'] = df['city'].astype(str).str.strip().str.lower() + '|' + df['state_code'].astype(str).str.strip().str.lower()
    
    # city_coords est un DataFrame qui contient les coordonnées (latitude et longitude) pour chaque clé de ville unique dans le DataFrame des shootings
    # dropna est utilisé pour s'assurer que nous ne prenons en compte que les villes qui ont des coordonnées valides
    # first() est utilisé pour prendre la première occurrence de chaque clé de ville, ce qui est suffisant pour remplir les coordonnées des shootings. eVITER des doublons dans city_coords
    
    city_coords = df.dropna(subset=['latitude','longitude']).groupby('city_key')[['latitude','longitude']].first()
    
    # Fusionner les coordonnées de la table des villes avec le DataFrame des shootings en utilisant la clé de ville
    df = df.merge(city_coords, left_on='city_key', right_index=True, how='left', suffixes=('', '_city'))
    
    # Remplir les coordonnées manquantes dans le DataFrame des shootings avec les coordonnées correspondantes de la table des villes
    df['latitude'] = df['latitude'].fillna(df['latitude_city'])
    df['longitude'] = df['longitude'].fillna(df['longitude_city'])
    
    # supprimer les colonnes intermédiaires utilisées pour le remplissage des coordonnées
    df.drop(columns=['city_key','latitude_city','longitude_city'], inplace=True)
    
    # Remplacer les valeurs manquantes de latitude et longitude par 0.0 
    df['latitude'] = df['latitude'].fillna(0.0)
    df['longitude'] = df['longitude'].fillna(0.0)
    
    # Renommer les colonnes latitude et longitude pour les différencier des coordonnées des villes
    df.rename(columns={'latitude': 'incident_latitude', 'longitude': 'incident_longitude'}, inplace=True)
    
    # Retourner le DataFrame avec les coordonnées remplies
    return df

def get_dtype_dict() -> Dict:
    """ Retourne un dictionnaire de types de données pour les colonnes du DataFrame des shootings, utilisé lors de l'écriture dans la base de données."""
    return {
        'id_shooting': Integer(),
        'name': String(255),
        'manner_of_death': String(50),
        'armed': String(100),
        'age': SmallInteger(),
        'gender': String(50),
        'race': String(50),
        'city': String(100),
        'state_code': String(5),
        'signs_of_mental_illness': SmallInteger(),
        'threat_level': String(50),
        'flee': String(50),
        'body_camera': SmallInteger(),
        'incident_longitude': Float(),
        'incident_latitude': Float(),
        'is_geocoding_exact': SmallInteger(),
        'date': Date()
    }

def save_to_db(df: pd.DataFrame, engine: Engine, dtype_dict: Dict) -> None:
    """ Enregistre le DataFrame des shootings nettoyé et enrichi dans la table silver.shootings_clean de la base de données PostgreSQL."""
    
    # Créer le schéma silver s'il n'existe pas déjà
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        
    # Enregistrer les données dans la table silver.shootings_clean en utilisant des chunks pour gérer les grandes quantités de données
    chunk_size = 500
    rows = 0
    start_time = time.time()
    
    # Utiliser tqdm pour afficher une barre de progression pendant l'insertion des données
    for start in tqdm(range(0, len(df), chunk_size)):
        end = start + chunk_size
        df.iloc[start:end].to_sql(
            'shootings_clean',
            con=engine,
            schema='silver',
            if_exists='append' if start > 0 else 'replace',
            index=False,
            method='multi',
            dtype=dtype_dict
        )
        rows += len(df.iloc[start:end])
    elapsed_time = time.time() - start_time
    print(f"Toutes les données ont été écrites en {elapsed_time:.2f} secondes. {rows} lignes insérées.")
    
    # Ajouter une clé primaire à la table silver.shootings_clean sur la colonne id_shooting
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE silver.shootings_clean
            DROP CONSTRAINT IF EXISTS shootings_clean_pkey;
            ALTER TABLE silver.shootings_clean
            ADD PRIMARY KEY (id_shooting);
        """))

def main() -> None:
    """ fonction main pour exécuter le processus de chargement, nettoyage, enrichissement et sauvegarde des données de shootings."""
    engine = get_engine()
    df = load_data(engine)
    df = clean_data(df)
    df = fill_coordinates(df)
    dtype_dict = get_dtype_dict()
    save_to_db(df, engine, dtype_dict)

if __name__ == "__main__":
    main()
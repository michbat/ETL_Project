import pandas as pd
import time
import os
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from typing import Dict
from sqlalchemy.types import Integer, String,SmallInteger

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

def build_dim_victim(df: pd.DataFrame) -> pd.DataFrame:
    """Construire le DataFrame de la dimension victim à partir de shootings_enriched."""
    # Colonnes à conserver
    cols = ['gender','race_label', 'age', 'age_band', 'signs_of_mental_illness']
    df_dim_victim = df[cols].copy()
    
    # Supprimer les doublons dans le DataFrame
    df_dim_victim = df_dim_victim.drop_duplicates(subset=cols).reset_index(drop=True)
    
    # Ajouter une clé primaire pour la dimension victim
    df_dim_victim['victim_key'] = range(1, len(df_dim_victim) + 1)
    
    # Mapping pour constuire des codes de race
    race_code_mapping = {
        'white': 'W',
        'black': 'B',
        'hispanic': 'H',
        'asian': 'A',
        'native american': 'N',
        'other': 'O',
        'unknown': 'U'
    }

    # Appliquer le mapping des codes de race sur les labels
    df_dim_victim['race_code'] = df_dim_victim['race_label'].map(race_code_mapping)

    # Réorganiser les colonnes dans l'ordre souhaité
    df_dim_victim = df_dim_victim[
        ['victim_key', 'gender', 'race_code', 'race_label', 'age', 'age_band', 'signs_of_mental_illness']
    ]
    
    return df_dim_victim

def get_dtype_dict() -> Dict:
    """Retourner un dictionnaire de types de données SQL compatibles pour la table dim_victim."""
    return {
        'victim_key': Integer(), 
        'gender': String(),
        'race_code': String(),
        'race_label': String(),
        'age': Integer(),
        'age_band': String(),
        'signs_of_mental_illness': SmallInteger()
    }
    
def save_to_db(df: pd.DataFrame, engine: Engine, dtype_dict: Dict) -> None:
    """Sauvegarder le DataFrame dans la table gold.dim_victim en utilisant des chunks."""
    # Créer le schema gold s'il n'existe pas déjà
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
    
    # Enregistrer les données dans la table gold.dim_victim en utilisant des chunks pour gérer les grandes quantités de données
    chunk_size: int = 500
    start_time: float = time.time()
    rows: int = 0
    # df = df.reset_index(drop=True)
    
    for start in tqdm(range(0, len(df), chunk_size)):
        end: int = start + chunk_size
        df.iloc[start:end].to_sql(
            'dim_victim',
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
    
    print(f"Toutes les données ont été écrites en {elapsed_time:.2f} secondes. {rows} lignes ont été insérées dans la table 'gold.dim_victim'")
    
    # Ajouter la contrainte de clé primaire sur victim_key après l'insertion des données
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE gold.dim_victim
            ADD CONSTRAINT pk_dim_victim PRIMARY KEY (victim_key)
        """))   
        
def main() -> None:
    """Fonction principale pour construire et sauvegarder la dimension victim."""
    engine = get_engine()
    df_shootings = load_data(engine)
    df_dim_victim = build_dim_victim(df_shootings)
    dtype_dict = get_dtype_dict()
    save_to_db(df_dim_victim, engine, dtype_dict)

if __name__ == "__main__":
    main()
import pandas as pd
import time
import os
from tqdm import tqdm
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.types import Integer, String, SmallInteger
from typing import Tuple, Dict

def get_engine() -> Engine:
    """Créer et retourner une instance Engine pour se connecter à la base PostgreSQL."""
    # query_string = 'postgresql+psycopg://admin:admin@localhost:5434/us_violent_incidents'
    
    host = os.environ.get('POSTGRES_HOST', 'postgres-db')
    port = os.environ.get('POSTGRES_PORT', '5432')
    user = os.environ['POSTGRES_USER']
    password = os.environ['POSTGRES_PASSWORD']
    db = os.environ['POSTGRES_DB']
    query_string = f'postgresql+psycopg://{user}:{password}@{host}:{port}/{db}'
    return create_engine(query_string)

def load_staging(engine: Engine) -> pd.DataFrame:
    """Charger les données de la table silver.shootings_enriched dans un DataFrame."""
    return pd.read_sql_query(text('SELECT * FROM silver.shootings_enriched'), con=engine)

def load_dimensions(engine: Engine) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Charger toutes les dimensions gold dans des DataFrames."""
    dim_date = pd.read_sql_table('dim_date', con=engine, schema='gold')
    dim_location = pd.read_sql_table('dim_location', con=engine, schema='gold')
    dim_victim = pd.read_sql_table('dim_victim', con=engine, schema='gold')
    dim_weapon = pd.read_sql_table('dim_weapon', con=engine, schema='gold')
    dim_circumstance = pd.read_sql_table('dim_circumstance', con=engine, schema='gold')
    return dim_date, dim_location, dim_victim, dim_weapon, dim_circumstance

def join_dimensions(
    df_staging: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_location: pd.DataFrame,
    dim_victim: pd.DataFrame,
    dim_weapon: pd.DataFrame,
    dim_circumstance: pd.DataFrame
) -> pd.DataFrame:
    """Effectuer les jointures entre la table de staging et toutes les dimensions."""
    # Jointure avec la dimension date
    df = df_staging.merge(dim_date, left_on='date', right_on='full_date', how='left')
    # Jointure avec la dimension location
    df = df.merge(dim_location, on=[
        'city', 'state_code', 'state_name', 'county_name', 'timezone',
        'city_latitude', 'city_longitude', 'incident_latitude', 'incident_longitude',
        'population', 'density', 'total_population_state', 'pct_white', 'pct_black',
        'pct_hispanic', 'pct_asian', 'pct_american_indian'
    ], how='left')
    # Jointure avec la dimension victim
    df = df.merge(dim_victim, on=[
        'gender', 'race_code', 'race_label', 'age', 'age_band', 'signs_of_mental_illness'
    ], how='left')
    # Jointure avec la dimension weapon
    df = df.merge(dim_weapon,
        left_on=['armed', 'weapon_category', 'armed_flag', 'unarmed_flag'],
        right_on=['armed_raw', 'weapon_category', 'is_armed', 'is_unarmed'],
        how='left'
    )
    # Jointure avec la dimension circumstance
    df = df.merge(dim_circumstance, on=[
        'manner_of_death', 'threat_level', 'flee', 'body_camera_flag', 'is_geocoding_exact'
    ], how='left')
    return df

def build_fact_table(df: pd.DataFrame) -> pd.DataFrame:
    """Construire le DataFrame de la table de faits fact_fatal_shootings."""
    # Colonnes à conserver et construction de la table de faits
    df_fact = pd.DataFrame({
        'date_key': df['date_key'],
        'location_key': df['location_key'],
        'victim_key': df['victim_key'],
        'weapon_key': df['weapon_key'],
        'circumstance_key': df['circumstance_key'],
        'source_shooting_id': df['id_shooting'],
        'shooting_count': 1,
        'armed_flag': df['armed_flag'],
        'unarmed_flag': df['unarmed_flag'],
        'body_camera_flag': df['body_camera_flag'],
        'mental_illness_flag': df['signs_of_mental_illness'],
        'flee_flag': df['flee_flag'],
        'exact_geocoding_flag': df['is_geocoding_exact']
    })
    # Réinitialiser l'index et ajouter la clé primaire
    df_fact.reset_index(drop=True, inplace=True)
    df_fact['fact_fatal_key'] = range(1, len(df_fact) + 1)
    # Réorganiser les colonnes dans l'ordre souhaité
    df_fact = df_fact[[
        'fact_fatal_key', 'date_key', 'location_key', 'victim_key', 'weapon_key', 'circumstance_key',
        'source_shooting_id', 'shooting_count', 'armed_flag', 'unarmed_flag',
        'body_camera_flag', 'mental_illness_flag', 'flee_flag', 'exact_geocoding_flag'
    ]]
    return df_fact

def get_dtypes_dict() -> Dict:
    """Retourner un dictionnaire de types SQL compatibles pour la table fact_fatal_shootings."""
    return {
        'fact_fatal_key': Integer(),
        'date_key': Integer(),
        'location_key': Integer(),
        'victim_key': Integer(),
        'weapon_key': Integer(),
        'circumstance_key': Integer(),
        'source_shooting_id': String(),
        'shooting_count': SmallInteger(),
        'armed_flag': SmallInteger(),
        'unarmed_flag': SmallInteger(),
        'body_camera_flag': SmallInteger(),
        'mental_illness_flag': SmallInteger(),
        'flee_flag': SmallInteger(),
        'exact_geocoding_flag': SmallInteger()
    }

def create_gold_schema(engine: Engine) -> None:
    """Créer le schéma gold s'il n'existe pas déjà."""
    with engine.connect() as conn:
        conn.execute(text('CREATE SCHEMA IF NOT EXISTS gold'))

def insert_fact_table(
    engine: Engine,
    df_fact: pd.DataFrame,
    dtypes_dict: Dict,
    chunk_size: int = 2000
) -> None:
    """Sauvegarder le DataFrame dans la table gold.fact_fatal_shootings en utilisant des chunks."""
    # Insertion par lots pour gérer les gros volumes
    start_time: float = time.time()
    rows: int = 0
    for start in tqdm(range(0, len(df_fact), chunk_size)):
        end: int = start + chunk_size
        df_fact.iloc[start:end].to_sql(
            'fact_fatal_shootings',
            con=engine,
            schema='gold',
            if_exists='append' if start > 0 else 'replace',
            index=False,
            method='multi',
            chunksize=chunk_size,
            dtype=dtypes_dict
        )
        rows += len(df_fact.iloc[start:end])
    elapsed_time: float = time.time() - start_time
    print(f"Toutes les données ont été écrites en {elapsed_time:.2f} secondes. {rows} lignes ont été insérées dans la table 'gold.fact_fatal_shootings'")

def add_primary_key(engine: Engine) -> None:
    """Ajouter la contrainte de clé primaire sur fact_fatal_shootings après insertion."""
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE gold.fact_fatal_shootings
            ADD CONSTRAINT pk_fact_fatal_shootings PRIMARY KEY (fact_fatal_key)
        """))

def main() -> None:
    """Fonction main pour construire et sauvegarder la table de faits fact_fatal_shootings."""
    engine = get_engine()
    create_gold_schema(engine)
    df_staging = load_staging(engine)
    dim_date, dim_location, dim_victim, dim_weapon, dim_circumstance = load_dimensions(engine)
    df = join_dimensions(df_staging, dim_date, dim_location, dim_victim, dim_weapon, dim_circumstance)
    df_fact = build_fact_table(df)
    dtypes_dict = get_dtypes_dict()
    insert_fact_table(engine, df_fact, dtypes_dict)
    add_primary_key(engine)

if __name__ == "__main__":
    main()
import pandas as pd
import time
import os
from tqdm import tqdm
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.types import Integer, SmallInteger
from typing import Dict

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

def load_dimensions(engine: Engine)-> tuple:
    """Charger les dimensions nécessaires dans des DataFrames."""
    dim_location = pd.read_sql_query(
    text("SELECT location_key, state_code, city FROM gold.dim_location"), engine)
    
    dim_state = pd.read_sql_query(
    text("SELECT state_key, state_code FROM gold.dim_state"), engine)
    
    dim_month = pd.read_sql_query(
    text("SELECT month_key, year, month FROM gold.dim_month"), engine)
    
    dim_date = pd.read_sql_query(
    text("SELECT date_key, year, month FROM gold.dim_date"), engine)
    
    dim_victim = pd.read_sql_query(
    text("SELECT victim_key, age, race_label, signs_of_mental_illness FROM gold.dim_victim"), engine)
    
    dim_circumstance = pd.read_sql_query(
    text("SELECT circumstance_key, threat_level FROM gold.dim_circumstance"), engine) 
    
    return dim_location, dim_state, dim_month, dim_date, dim_victim, dim_circumstance

def load_fact_shootings(engine: Engine) -> pd.DataFrame:
    """Charger la table de faits fact_fatal_shootings."""
    fact_shootings = pd.read_sql_query(
    text("""
        SELECT
            fact_fatal_key,
            date_key,
            location_key,
            victim_key,
            circumstance_key,
            source_shooting_id,
            shooting_count,
            armed_flag,
            unarmed_flag,
            body_camera_flag,
            flee_flag
        FROM gold.fact_fatal_shootings
    """),
    engine)
    
    return fact_shootings

def build_fact_state_month_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Construire le DataFrame d'agrégats par état et mois, avec la granularité source_shooting_id."""
    aggregate_df = df.groupby(['state_key', 'month_key', 'source_shooting_id']).agg(
        shootings_count=('shooting_count', 'sum'),
        armed_count=('armed_flag', 'sum'),
        unarmed_count=('unarmed_flag', 'sum'),
        body_camera_count=('body_camera_flag', 'sum'),
        mental_illness_count=('signs_of_mental_illness', 'sum'),
        flee_count=('flee_flag', 'sum'),
        attack_treat_count=('threat_level', lambda x: (x == 'attack').sum()),
        other_treat_count=('threat_level', lambda x: (x == 'other').sum()),
        avg_age=('age', 'mean'),
        distinct_cities_count=('city', 'nunique'),
        white_victim_count=('race_label', lambda x: (x == 'white').sum()),
        black_victim_count=('race_label', lambda x: (x == 'black').sum()),
        hispanic_victim_count=('race_label', lambda x: (x == 'hispanic').sum()),
        asian_victim_count=('race_label', lambda x: (x == 'asian').sum()),
        native_american_victim_count=('race_label', lambda x: (x == 'native american').sum()),
        not_specified_race_victim_count=('race_label', lambda x: (x == 'not specified').sum())
    ).reset_index()
    
    # Arrondir la moyenne d'âge et convertir en entier
    aggregate_df['avg_age'] = aggregate_df['avg_age'].round().astype('Int64')
    
    # Ajouter une clé primaire fact_state_month_key auto-incrémentée
    aggregate_df['fact_state_month_key'] = range(1, len(aggregate_df) + 1)
    
    # Réorganiser les colonnes pour que fact_state_month_key soit en premier
    cols = ['fact_state_month_key'] + [col for col in aggregate_df.columns if col != 'fact_state_month_key']
    return aggregate_df[cols]

def get_dtypes_dict() -> Dict:
    """Retourner un dictionnaire de types SQL pour la table fact_state_month_metrics."""
    return {
        'fact_state_month_key': Integer(),
        'state_key': Integer(),
        'month_key': Integer(),
        'source_shooting_id': Integer(),
        'shootings_count': SmallInteger(),
        'armed_count': SmallInteger(),
        'unarmed_count': SmallInteger(),
        'body_camera_count': SmallInteger(),
        'mental_illness_count': SmallInteger(),
        'flee_count': SmallInteger(),
        'attack_treat_count': SmallInteger(),
        'other_treat_count': SmallInteger(),
        'avg_age': SmallInteger(),
        'distinct_cities_count': SmallInteger(),
        'white_victim_count': SmallInteger(),
        'black_victim_count': SmallInteger(),
        'hispanic_victim_count': SmallInteger(),
        'asian_victim_count': SmallInteger(),
        'native_american_victim_count': SmallInteger(),
        'not_specified_race_victim_count': SmallInteger()
    }

def create_gold_schema(engine: Engine) -> None:
    """Créer le schéma gold s'il n'existe pas déjà."""
    with engine.connect() as conn:
        conn.execute(text('CREATE SCHEMA IF NOT EXISTS gold'))

def insert_fact_table(engine: Engine, df_fact: pd.DataFrame, dtypes_dict: Dict) -> None:
    """Sauvegarder le DataFrame dans la table gold.fact_state_month_metrics en utilisant des chunks."""
    chunk_size:int = 500
    start_time: float = time.time()
    rows: int = 0
    for start in tqdm(range(0, len(df_fact), chunk_size)):
        end: int = start + chunk_size
        df_fact.iloc[start:end].to_sql(
            'fact_state_month_metrics',
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
    print(f"Toutes les données ont été écrites en {elapsed_time:.2f} secondes. {rows} lignes ont été insérées dans la table 'gold.fact_state_month_metrics'")

def add_primary_key(engine: Engine) -> None:
    """Ajouter la contrainte de clé primaire sur fact_state_month_metrics après insertion."""
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE gold.fact_state_month_metrics
            ADD CONSTRAINT pk_fact_state_month_metrics PRIMARY KEY (fact_state_month_key)
        """))

def main() -> None:
    engine = get_engine()
    create_gold_schema(engine)
    dim_location, dim_state, dim_month, dim_date, dim_victim, dim_circumstance = load_dimensions(engine)
    fact_shootings = load_fact_shootings(engine)
    
    # Jointures
    df = fact_shootings.merge(dim_location, on='location_key', how='left')
    df = df.merge(dim_state, on='state_code', how='left')
    df = df.merge(dim_date, on='date_key', how='left')
    df = df.merge(dim_month, on=['year', 'month'], how='left')
    df = df.merge(dim_victim, on='victim_key', how='left')
    df = df.merge(dim_circumstance, on='circumstance_key', how='left')

    aggregate_df = build_fact_state_month_metrics(df)
    dtypes_dict = get_dtypes_dict()
    insert_fact_table(engine, aggregate_df, dtypes_dict)
    add_primary_key(engine)

if __name__ == "__main__":
    main()

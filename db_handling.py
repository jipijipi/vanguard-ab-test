from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text
import pandas as pd

# something
def create_db(password, config):
    database_name = config['database_name']

    # Set Up Database Connection
    engine = create_engine(f'mysql+pymysql://root:{password}@localhost')

    # Create Database if it Doesn't Exist
    with engine.connect() as conn:
        conn.execute(text(f'CREATE DATABASE IF NOT EXISTS {database_name}'))

    # Connect to the Newly Created Database
    engine = create_engine(
        f'mysql+pymysql://root:{password}@localhost/{database_name}')
    return engine


def export_dataframes_to_sql(engine, dataframes):

    for table_name, df in dataframes.items():
        df.to_sql(name=table_name, con=engine,
                  if_exists='replace', index=False)


def import_data_from_sql(engine, table_name):
    data = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    return data


def import_all_tables_from_sql(engine):
    dataframes = {}
    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    for table_name in table_names:
        dataframes[table_name] = import_data_from_sql(engine, table_name)

    return dataframes


def export_dataframes_to_csv(dataframes):
    for table in dataframes:
        dataframes[table].to_csv(f'data/cleaned/{table}.csv', index=False)

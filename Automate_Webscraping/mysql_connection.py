from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd

import os
from dotenv import load_dotenv
import logging

load_dotenv()

DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def table_df(database_name, table_name):
    conn_url = URL.create(
        "mysql+mysqlconnector",
        username=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=database_name
    )

    engine = create_engine(conn_url)
    con = engine.connect()

    query = f'SELECT * FROM {table_name}'
    # df = pd.read_sql(sql=query, con=con)                  # Pandas < 2.2.0
    df = pd.read_sql(sql=query, con=con.connection)         # Pandas >= 2.2.0
    con.close()  
    return df

def df_table(dataframe, database_name, table_name):
    try:
        conn_url = URL.create(
            "mysql+mysqlconnector",
            username=DB_USERNAME,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=database_name
        )

        logger.info(f"Connection URL: {conn_url}")

        engine = create_engine(conn_url)
        with engine.connect() as con:
            # dataframe.to_sql(table_name, con=con.connection, if_exists='replace', index=False, chunksize=100000)
            dataframe.to_sql(table_name, con=con.connection, if_exists='replace', index=False, chunksize=100000)
            logger.info(f"Dataframe successfully written to table {table_name} in database {database_name}.")
    except Exception as e:
        logger.error(f"Error occurred: {e}")

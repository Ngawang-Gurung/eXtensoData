import pandas as pd

# SQL 

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

def table_df(database_name, table_name):

    conn_url = URL.create(
        "mysql+mysqlconnector",
        username = "root",
        password = "mysql@123",
        host = "localhost",
        port = 3306,
        database = database_name) 
    
    engine = create_engine(conn_url)
    con = engine.connect()

    query = f'SELECT * FROM {table_name}'
    df = pd.read_sql(sql=query, con=con)
    return df

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd

def table_df(database_name, table_name):

    conn_url = URL.create(
        "mysql+mysqlconnector",
        username = "root",
        password = "mysql@123",
        host = "localhost",
        port = 3306,
        database = database_name) 

    # conn_url = f"mysql+mysqlconnector://root:mysql%40123@localhost:3306/{database_name}"
    engine = create_engine(conn_url)
    con = engine.connect()

    query = f'SELECT * FROM {table_name}'
    df = pd.read_sql(sql=query, con=con)
    return df

def df_table(database_name, dataframe, table_name):
    
    conn_url = URL.create(
        "mysql+mysqlconnector",
        username = "root",
        password = "mysql@123",
        host = "localhost",
        port = 3306,
        database = database_name) 
    
    engine = create_engine(conn_url)
    con = engine.connect()
    dataframe.to_sql(table_name, con=con, if_exists='replace', index=False)  
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd

# SQL

import os
from dotenv import load_dotenv

load_dotenv()

DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT'))

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
    df = pd.read_sql(sql=query, con=con)
    con.close()  # Close the connection after use
    return df

def df_table(dataframe, database_name, table_name):
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
    dataframe.to_sql(table_name, con=con, if_exists='replace', index=False)
    con.close()  # Close the connection after use


# AES

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from base64 import b64encode, b64decode

def aes_encrypt(text, key):
    key_bytes = key.encode('utf-8')  
    cipher = AES.new(key_bytes, AES.MODE_ECB)
    padded_text = pad(text.encode(), AES.block_size)
    encrypted_text = cipher.encrypt(padded_text)
    return b64encode(encrypted_text).decode()

def aes_decrypt(ciphertext, key):
    key_bytes = key.encode('utf-8')  
    cipher = AES.new(key_bytes, AES.MODE_ECB)
    decrypted_text = cipher.decrypt(b64decode(ciphertext))
    unpadded_text = unpad(decrypted_text, AES.block_size)
    return unpadded_text.decode()

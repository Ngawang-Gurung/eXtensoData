from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pymysql

import os
from dotenv import load_dotenv

load_dotenv()

DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

spark = SparkSession.builder.appName("cf_etl").getOrCreate()

def table_df(schema_name, table_name):
    url = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{schema_name}"
    properties = {
        "user": DB_USERNAME,
        "password": DB_PASSWORD,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    df = spark.read.jdbc(url=url, table=table_name, properties=properties)
    return df


def update_date_on_config_table(schema_name, table_name, index):
    '''
    Update dates on config table using PyMySQL 
    '''

    pymysql_connection = pymysql.connect(
        host='localhost',
        user='root',
        password='mysql@123',
        database= schema_name
    )

    with pymysql_connection.cursor() as cursor:
        exec_date_query = f"update `{schema_name}`.{table_name} set execution_date = (current_timestamp) where id = {index+1}"
        cursor.execute(exec_date_query)

        update_startdate_query = f"update `{schema_name}`.{table_name}  set start_date_time = date_add(start_date_time, interval 1 day)"
        cursor.execute(update_startdate_query)

        update_enddate_query = f"update `{schema_name}`.{table_name}  set end_date_time = date_add(end_date_time, interval 1 day)"
        cursor.execute(update_enddate_query)
        
        pymysql_connection.commit()


def upload():
    '''
    Appends table if incremental is true else overwrites in HDFS
    '''

    df = table_df('migration','cf_etl_table')

    for i, row in zip(range(df.count()), df.collect()):
        is_incremental, schema, table, location, hdfs_file = row['is_incremental'], row['schema_name'], row['table_name'], row['hdfs_upload_location'], row['hdfs_file_name'] 
        hdfs_path = f"{location}{hdfs_file}"
        
        if is_incremental:
            start_date, end_date, date_col = row['start_date_time'], row['end_date_time'], row['inc_field']
 
            query = f"(SELECT * FROM {schema}.{table} WHERE {date_col} BETWEEN '{start_date}' AND '{end_date}') AS sql_query"
            result = table_df(schema, query)
            result.write.mode('append').parquet(hdfs_path)   
            update_date_on_config_table('migration', 'cf_etl_table', i)
            print("Incremental load into HDFS is successful")

        elif not is_incremental:
            result = table_df(schema, table)
            result.write.mode("overwrite").parquet(hdfs_path)
            print("Non-incremental load into HDFS is successful")

if __name__ == '__main__': 
    upload()




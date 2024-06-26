from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pymysql

spark = SparkSession.builder.appName("cf_etl").getOrCreate()

def table_df(schema_name, table_name):
    '''
    MySQL and Spark Connections    
    '''
    url = f"jdbc:mysql://localhost/{schema_name}"
    properties = {
        "user": "root",
        "password": "mysql@123",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    df = spark.read.jdbc(url=url, table=table_name, properties=properties)
    return df

def update_date_on_config_table(schema_name, table_name, index, interval_period):
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
        exec_date_query = f"update `{schema_name}`.{table_name} set execution_date = (current_timestamp) where table_id = {index+1}"
        cursor.execute(exec_date_query)

        update_startdate_query = f"update `{schema_name}`.{table_name}  set start_date_time = date_add(start_date_time, interval {interval_period} day)"
        cursor.execute(update_startdate_query)

        update_enddate_query = f"update `{schema_name}`.{table_name}  set end_date_time = date_add(end_date_time, interval {interval_period} day)"
        cursor.execute(update_enddate_query)
        
        pymysql_connection.commit()


def field_mapped_df(cf_db, schema_name, table_name, table_id):

    con = pymysql.connect(
        host='localhost',
        user='root',
        password='mysql@123',
        database= cf_db
    )

    with con.cursor() as cursor:
        cursor.callproc(f'{cf_db}.sp_field_mapping', [schema_name, table_name, table_id])
        result = cursor.fetchall()
        fields = [desc[0] for desc in cursor.description]   
        df = spark.createDataFrame(result, fields)    
        con.commit()
    
    return df


def upload():

    df = table_df('config_db','cf_etl_table')

    for i, row in zip(range(df.count()), df.collect()):
        is_incremental, table_id, schema, table, location, hdfs_file = row['is_incremental'], row['table_id'], row['schema_name'], row['table_name'], row['hdfs_upload_location'], row['hdfs_file_name'] 
        hdfs_path = f"{location}{hdfs_file}"

        field_mapped_table = field_mapped_df('config_db', schema, table, table_id)
        
        if is_incremental:
            start_date, end_date, date_col, interval_period, partition_by = row['start_date_time'], row['end_date_time'], row['inc_field'], row['interval_period'], row['partition_by']

            field_mapped_table.createOrReplaceTempView("incremental_table")
            result = spark.sql(f"SELECT * FROM incremental_table WHERE {date_col} BETWEEN '{start_date}' AND '{end_date}'")
            result.write.mode('append').parquet(hdfs_path, partitionBy = partition_by)
            
            update_date_on_config_table('config_db', 'cf_etl_table', i, interval_period)

        elif not is_incremental:
            field_mapped_table.write.mode("overwrite").parquet(hdfs_path)

if __name__ == '__main__': 
    upload()




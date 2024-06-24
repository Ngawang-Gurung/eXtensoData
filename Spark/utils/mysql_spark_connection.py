from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("mysql_spark_connector").getOrCreate()

def table_df(schema_name, table_name):
    url = f"jdbc:mysql://localhost/{schema_name}"
    properties = {
        "user": "root",
        "password": "mysql@123",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    df = spark.read.jdbc(url=url, table=table_name, properties=properties)
    return df

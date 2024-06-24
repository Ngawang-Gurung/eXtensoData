from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Load Dataset

spark = SparkSession.builder \
    .appName("Exponential_Smoothing") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
    .getOrCreate()

data = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("./dataset/test.xlsx")

# # Aggregating by account_id and month_bs_id

df = data.groupBy('account_id', 'month_bs_id').agg(
    sum('txn_amt').alias('total_txn_amt'),
    sum('txn_cnt').alias('total_txn_cnt')
).orderBy(asc('account_id'), asc('month_bs_id'))


# # Exponential Smoothing Function

@udf(returnType=FloatType())
def exponential_smoothing(series, alpha):
    result = [series[0]]  
    for i in range(1, len(series)):
        result.append(alpha * series[i] + (1 - alpha) * result[-1])
    return result[-1] 

window_spec = Window.partitionBy("account_id").orderBy("month_bs_id")

smoothing_factor = 0.6
# smoothing_factor = 2/(2+1)

df = df.withColumn(
    "smoothed_txn_amt", 
    exponential_smoothing(collect_list("total_txn_amt").over(window_spec), lit(smoothing_factor))
)

df = df.withColumn(
    "smoothed_txn_cnt", 
    exponential_smoothing(collect_list("total_txn_cnt").over(window_spec), lit(smoothing_factor))
)

print(df.show())
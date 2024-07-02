from pyspark.sql.functions import *
from pyspark.sql.window import Window
from utils.mysql_spark_connection import table_df

rw_transaction_data = table_df('customer', 'rw_transaction_data')
product_category_map = table_df('customer', 'product_category_map')

df = rw_transaction_data.join(product_category_map, on = ['product_id', 'product_type_id', 'module_id'], how='inner')

# Date Operations
df = df.withColumn("last_modified_date", col("last_modified_date").cast("string"))
df = df.withColumn("dates", concat_ws(" ", col("last_modified_date"), col("time")))
df = df.withColumn('dates', to_timestamp('dates', 'yyyy-MM-dd HH:mm:ss'))
df = df.withColumn("last_modified_date", to_date('last_modified_date', 'yyyy-MM-dd'))
df = df.withColumn("month", month('last_modified_date'))

# Txn Flow
grouped_df = df.groupBy(['payer_account_id', 'month', 'txn_flow']).agg(count('amount').alias('count'), sum('amount').alias('sum'))

pivot_grouped_df_sum = grouped_df.groupBy('payer_account_id').pivot('txn_flow').agg(sum('sum').alias('total_amount'), avg('sum').alias('monthly_amount'))
pivot_grouped_df_count = grouped_df.groupBy('payer_account_id').pivot('txn_flow').agg(sum('count').alias('total_count'), avg('count').alias('monthly_count'))

# Reveneue
reward_agg = df.groupBy('payer_account_id').agg(sum('reward_point').alias('reward_point'))
monthly_revenue = df.groupBy(['payer_account_id', 'month']).agg(sum('revenue_amount').alias('revenue_amount'))
revenue = monthly_revenue.groupBy('payer_account_id').agg(avg('revenue_amount').alias('monthly_average_lifetime_revenue'), sum('revenue_amount').alias('total_revenue'))

# Latest Trans Date and Product Usage
latest_trans_date = df.groupBy('payer_account_id').agg(max('dates').alias('latest_transaction_date'))

latest = df.join(latest_trans_date, on=latest_trans_date['latest_transaction_date'] == df['dates'], how = 'semi').select(df['payer_account_id'], df['dates'].alias('latest_transaction_date'), df['product_name'].alias('latest_used_product'))
latest = latest.dropDuplicates(['payer_account_id'])

# Product Usage
product_usage = df.groupBy('payer_account_id').agg(count('product_id').alias('product usage'))

# Nth Product 
product_counts = df.groupBy('payer_account_id', 'product_name').agg(count('product_name').alias('product usage'))
ranked_products = product_counts.withColumn("row_number", row_number().over(Window.partitionBy("payer_account_id").orderBy(col("product usage").desc())))

nth_result_df = ranked_products.groupBy("payer_account_id").agg(
    max(when(col("row_number") == 1, col("product_name"))).alias("most_used_product"),
    max(when(col("row_number") == 2, col("product_name"))).alias("second_most_used_product"),
    max(when(col("row_number") == 3, col("product_name"))).alias("third_most_used_product")
)

final_result = (
    pivot_grouped_df_sum
    .join(pivot_grouped_df_count, on='payer_account_id')
    .join(reward_agg, on='payer_account_id')
    .join(revenue, on='payer_account_id')
    .join(latest, on='payer_account_id')
    .join(product_usage, on='payer_account_id')
    .join(nth_result_df, on='payer_account_id')
)

print(final_result.show())


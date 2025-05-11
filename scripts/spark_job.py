from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
import os

spark = SparkSession.builder \
    .appName("Check Parquet Files") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.jars", "/opt/airflow/jars/spark-bigquery-with-dependencies_2.12-0.42.1.jar,/opt/airflow/jars/gcs-connector-hadoop3-latest.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .getOrCreate()

parquet_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("short_description", StringType(), True),
    StructField("price", LongType(), True),
    StructField("list_price", LongType(), True),
    StructField("discount", LongType(), True),
    StructField("discount_rate", LongType(), True),
    StructField("all_time_quantity_sold", DoubleType(), True),
    StructField("inventory_status", StringType(), True),
    StructField("stock_item_qty", LongType(), True),
    StructField("stock_item_max_sale_qty", LongType(), True)
])

df = spark.read.schema(parquet_schema).parquet("hdfs://namenode:9000/user/airflow/details/product_inmemory_20250416_074112.parquet")
df.printSchema() 
df.show(truncate=True)     

df.write \
    .format("bigquery") \
    .option("table", "tiki_data.product_data") \
    .option("temporaryGcsBucket", "tiki-spark-temp") \
    .option("writeDisposition", "WRITE_APPEND") \
    .mode("append") \
    .save()

spark.stop()
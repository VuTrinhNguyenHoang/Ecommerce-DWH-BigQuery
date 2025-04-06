from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType

spark = SparkSession.builder \
    .appName("Check Parquet Files") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

parquet_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("short_description", StringType(), True),
    StructField("price", LongType(), True),
    StructField("list_price", LongType(), True),
    StructField("discount", LongType(), True),
    StructField("discount_rate", LongType(), True),
    StructField("all_time_quantity_sold", LongType(), True),
    StructField("inventory_status", StringType(), True),
    StructField("stock_item_qty", LongType(), True),
    StructField("stock_item_max_sale_qty", LongType(), True)
])

df = spark.read.schema(parquet_schema).parquet("hdfs://namenode:9000/user/airflow/details/")
df.printSchema() 
df.show(truncate=True)       





from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from pyspark.sql.functions import col, trim
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--path', required=True)
args = parser.parse_args()

spark = SparkSession.builder \
        .appName("Clean Parquet Files") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

df = spark.read.parquet(args.path)
df.printSchema() 

df = df.na.fill({
    'name': 'unknown',
    'short_description': 'unknown',
    'inventory_status': 'unknown',
    'price': 0,
    'list_price': 0,
    'discount': 0,
    'discount_rate': 0,
    'all_time_quantity_sold': 0,
    'stock_item_qty': 0,
    'stock_item_max_sale_qty': 0
})

df = df.withColumn('name', trim(col('name'))) \
    .withColumn('short_description', trim(col('short_description'))) \
    .withColumn('inventory_status', trim(col('inventory_status')))

df = df.withColumn('all_time_quantity_sold', col('all_time_quantity_sold').cast('long'))

df = df.dropDuplicates(['id'])

df.show(5, truncate=True)
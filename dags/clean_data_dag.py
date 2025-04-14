from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

def clean_data():
    spark = SparkSession.builder \
        .appName("Clean Data") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

    df = spark.read.parquet("hdfs://namenode:9000/user/airflow/details/")

    df_clean = df.dropna(subset=["id", "name", "price", "list_price", "inventory_status"]) \
        .dropDuplicates() \
        .filter(
            (col("price") >= 0) &
            (col("list_price") >= 0) &
            (col("discount") >= 0) &
            (col("discount_rate") >= 0) &
            (col("all_time_quantity_sold") >= 0) &
            (col("stock_item_qty") >= 0) &
            (col("stock_item_max_sale_qty") >= 0)
        ) \
        .withColumn("name", trim(col("name"))) \
        .withColumn("short_description", trim(col("short_description"))) \
        .withColumn("inventory_status", trim(col("inventory_status")))

    # df_clean.write.mode("overwrite").parquet("hdfs://namenode:9000/user/airflow/cleaned_details/")

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG('clean_data',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )

    clean_data_task

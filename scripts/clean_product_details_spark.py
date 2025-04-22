from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import logging

def setup_logging():
    import logging
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        level=logging.INFO,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def create_spark_session(app_name="DataCleaning"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_data(spark, input_path):
    try:
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
            StructField("stock_item_max_sale_qty", LongType(), True),
            StructField("date", StringType(), True)
        ])

        df = spark.read.schema(parquet_schema).parquet(input_path)
        
        logger.info(f"Successfully read data from {input_path}")
        logger.info(f"Schema: {df.schema.json()}")
        logger.info(f"Total records: {df.count():,}")
        
        return df
    except Exception as e:
        logger.error(f"Failed to read data: {str(e)}")
        raise

def clean_data(df):
    try:
        # 1. Xử lý missing values
        df_clean = df.fillna({
            'price': 0.0,
            'list_price': 0.0,
            'discount': 0.0,
            'discount_rate': 0.0,
            'all_time_quantity_sold': 0,
            'stock_item_qty': 0,
            'stock_item_max_sale_qty': 0,
            'short_description': '',
            'inventory_status': 'unknown'
        })
        df_clean = df.dropna(subset=["name"])
        
        # 2. Chuẩn hóa kiểu dữ liệu
        df_clean = df_clean.withColumn(
            "price", 
            col("price").cast(DoubleType())
        ).withColumn(
            "all_time_quantity_sold", 
            col("all_time_quantity_sold").cast(LongType())
        ).withColumn(
            "date", 
            to_date(col("date"), "yyyy-MM-dd"))
        
        # 3. Xóa duplicates
        df_clean = df_clean.dropDuplicates(["id"])
        
        # 4. Validate business rules
        invalid_records = df_clean.filter(
            (col("price") < 0) |
            (col("all_time_quantity_sold") < 0)
        ).count()
        
        if invalid_records > 0:
            logger.warning(f"Found {invalid_records} invalid records violating business rules")
        
        return df_clean
    except Exception as e:
        logger.error(f"Data cleaning failed: {str(e)}")
        raise

def analyze_data(df):
    try:
        total_records = df.count()
        logger.info("\n=== Data Quality Report ===")
        logger.info(f"Total records: {total_records:,}")
        
        # 1. Phân tích missing values
        missing_values = df.select([
            count(when(col(c).isNull(), c)).alias(c) 
            for c in df.columns
        ]).collect()[0]
        
        logger.info("\nMissing Values Count:")
        for col_name in df.columns:
            logger.info(f"{col_name}: {missing_values[col_name]:,}")
        
        # 2. Phân tích giá cả
        price_stats = df.select(
            mean("price").alias("avg_price"),
            min("price").alias("min_price"),
            max("price").alias("max_price"),
            stddev("price").alias("stddev_price")
        ).collect()[0]
        
        logger.info("\nPrice Statistics:")
        logger.info(f"Average: {price_stats['avg_price']:.2f}")
        logger.info(f"Min: {price_stats['min_price']:.2f}")
        logger.info(f"Max: {price_stats['max_price']:.2f}")
        logger.info(f"Standard Deviation: {price_stats['stddev_price']:.2f}")
        
        # 3. Phân phối trạng thái hàng tồn kho
        logger.info("\nInventory Status Distribution:")
        df.groupBy("inventory_status").count().orderBy("count", ascending=False).show(truncate=False)
        
        # 4. Phân tích số lượng bán hàng
        sales_stats = df.select(
            sum("all_time_quantity_sold").alias("total_sold"),
            mean("all_time_quantity_sold").alias("avg_sold"),
            max("all_time_quantity_sold").alias("max_sold")
        ).collect()[0]
        
        logger.info("\nSales Statistics:")
        logger.info(f"Total items sold: {sales_stats['total_sold']:,}")
        logger.info(f"Average per product: {sales_stats['avg_sold']:.1f}")
        logger.info(f"Max sold for a product: {sales_stats['max_sold']:,}")
        
        # 5. Vi phạm quy tắc kinh doanh
        invalid_price = df.filter(col("price") < 0).count()
        invalid_quantity = df.filter(col("all_time_quantity_sold") < 0).count()
        
        logger.info("\nBusiness Rule Violations:")
        logger.info(f"Products with negative price: {invalid_price}")
        logger.info(f"Products with negative quantity sold: {invalid_quantity}")
        
        # 6. Check Duplicate 
        duplicate_ids = df.groupBy("id").count().filter("count > 1")
        logger.info(f"\nDuplicate IDs found: {duplicate_ids.count()}")
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise

def main():
    input_path = f"hdfs://namenode:9000{sys.argv[1]}"
    
    try:
        # 1. Spark Init
        spark = create_spark_session()
        logger.info("Spark session initialized")
        
        # 2. Read data
        df = read_data(spark, input_path)
        
        # 3. Clean data
        df_clean = clean_data(df)

        # 4. Analyze data
        analyze_data(df_clean)
        
        # 5. Show cleaned data
        print(df_clean.show(truncate=True))

        df_clean.write \
                .format("bigquery") \
                .option("table", "tiki_data.products") \
                .option("temporaryGcsBucket", "tiki-data-temp") \
                .option("writeDisposition", "WRITE_APPEND") \
                .mode("append") \
                .save()
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
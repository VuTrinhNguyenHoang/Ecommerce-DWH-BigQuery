from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
from datetime import datetime

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

def create_spark_session():
    spark = SparkSession.builder \
        .appName("ProductCommentsCleaning") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_data(spark, input_path):
    try:
        parquet_schema = StructType([
            StructField("id", LongType(), True),
            StructField("product_id", LongType(), True),
            StructField("title", StringType(), True),
            StructField("content", StringType(), True),
            StructField("rating", LongType(), True),
            StructField("created_at", LongType(), True),
            StructField("purchased_at", LongType(), True),
            StructField("date", StringType(), True)
        ])

        df = spark.read.schema(parquet_schema).parquet(input_path)
        
        logger.info(f"Successfully read {df.count()} raw comments")
        return df
    except Exception as e:
        logger.error(f"Failed to read data: {str(e)}")
        raise

def clean_comments(df):
    try:
        # 1. Xử lí giá trị null
        df_clean = (
            df.withColumn("title", when(col("title").isNull(), "unknown").otherwise(col("title")))
            .withColumn("content", when(col("content").isNull() | (col("content") == "") | (col("content") == "null"), "unknown").otherwise(col("content")))
            .dropna(subset=["product_id", "rating"])
        )
        
        # 2. Chuyển đổi cột timestamp
        df_clean = df_clean.withColumn(
            "created_at",
            from_unixtime(col("created_at")).cast(TimestampType())
        ).withColumn(
            "purchased_at",
            from_unixtime(col("purchased_at")).cast(TimestampType())
        ).withColumn(
            "date", 
            to_date(col("date"), "yyyy-MM-dd"))
        
        # 3. Chuyển các đánh giá thành dạng cảm xúc
        sentiment_expr = """
            CASE 
                WHEN title LIKE 'Cực kì hài lòng' THEN 'positive'
                WHEN title LIKE 'Hài lòng' THEN 'positive'
                WHEN title LIKE 'Bình thường' THEN 'neutral'
                WHEN title LIKE 'Không hài lòng' THEN 'negative'
                WHEN title LIKE 'Rất không hài lòng' THEN 'negative'
                ELSE 'unknown'
            END
        """
        df_clean = df_clean.withColumn("sentiment", expr(sentiment_expr))
        
        # 4. Tính số ngày mà được review
        df_clean = df_clean.withColumn(
            "days_to_review",
            abs(datediff(col("created_at"), col("purchased_at")))
        )
        
        # 5. Kiểm tra rating_valid
        df_clean = df_clean.withColumn(
            "rating_valid",
            (col("rating") >= 1) & (col("rating") <= 5)
        )
        
        # 6. Add processing metadata
        df_clean = df_clean.withColumn(
            "processing_date",
            lit(datetime.now().strftime("%Y-%m-%d"))
        )
        
        logger.info("Data cleaning completed successfully")
        return df_clean
        
    except Exception as e:
        logger.error(f"Cleaning failed: {str(e)}")
        raise

def analyze_data(df):
    try:
        total_records = df.count()
        cleaned_records = df.filter(col("rating_valid") == True).count()
        unknown_content = df.filter(col("content") == "unknown").count()

        rating_dist = df.groupBy("rating").count().orderBy("rating")
        
        sentiment_dist = df.groupBy("sentiment").count()
        
        logger.info("\n=== Data Quality Report ===")
        logger.info(f"Total records: {total_records}")
        logger.info(f"Valid ratings: {cleaned_records} ({cleaned_records/total_records:.1%})")
        logger.info(f"Unknown content: {unknown_content}")
        
        logger.info("\nRating Distribution:")
        for row in rating_dist.collect():
            logger.info(f"Rating {row['rating']}: {row['count']}")
            
        logger.info("\nSentiment Distribution:")
        for row in sentiment_dist.collect():
            logger.info(f"{row['sentiment']}: {row['count']}")
            
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise

def write_output(df, output_path):
    try:
        (df.write
           .mode("overwrite")
           .partitionBy("date")
           .parquet(output_path))
        
        logger.info(f"Successfully wrote cleaned data to {output_path}")
    except Exception as e:
        logger.error(f"Failed to write output: {str(e)}")
        raise

def main():
    try:
        input_path = f"hdfs://namenode:9000/{sys.argv[1]}"
        
        # 1. Spark Init
        spark = create_spark_session()
        logger.info("Spark session initialized")
        
        # 2. Read data
        df = read_data(spark, input_path)
        
        # 3. Clean data
        df_clean = clean_comments(df)
        
        # 4. Analyze data quality
        analyze_data(df_clean)
        
        # 5. Read cleaned data
        print(df_clean.show(truncate=True))
        
        df_clean.write \
            .format("bigquery") \
            .option("table", "tiki_data.comments") \
            .option("temporaryGcsBucket", "tiki-spark-temp") \
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

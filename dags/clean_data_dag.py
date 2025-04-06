from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
print("JAVA_HOME =", os.environ.get("JAVA_HOME"))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='clean_data_spark_dag',
    default_args=default_args,
    description='DAG chạy Spark để làm sạch dữ liệu',
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False
) as dag:
    clean_data_task = SparkSubmitOperator(
        task_id='spark_clean_data',
        application='/opt/airflow/scripts/clean_data_spark.py',  
        conn_id='spark_default', 
        verbose=True,
        dag=dag
    )

    clean_data_task
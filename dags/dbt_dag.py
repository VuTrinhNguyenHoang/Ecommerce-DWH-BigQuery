from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 25),
    'retries': 0,
}

with DAG(
    'dbt_bigquery_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/dbt/dbt-bigquery-project/ecommerce_dwh && dbt run',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/dbt/dbt-bigquery-project/ecommerce_dwh && dbt test',
    )

    dbt_run >> dbt_test
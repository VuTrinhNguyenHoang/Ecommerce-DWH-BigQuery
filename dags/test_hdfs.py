from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

def upload_file_to_hdfs(**kwargs):

    hook = WebHDFSHook(webhdfs_conn_id='webhdfs_default')
    
    local_file = '/opt/airflow/dags/categories/categories_id.parquet'
    remote_file = '/user/airflow/categories/categories_id.parquet'
    
    client = hook.get_conn()
    client.upload(remote_file, local_file, overwrite=True)
    print(f"File {local_file} đã được upload lên HDFS tại {remote_file}")


with DAG(
    dag_id='upload_parquet_to_hdfs_custom',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id='upload_file_to_hdfs',
        python_callable=upload_file_to_hdfs,
        provide_context=True
    )

    upload_task

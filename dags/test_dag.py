from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bash_command_dag',
    default_args=default_args,
    schedule_interval='@daily', 
    catchup=False
) as dag:
    
    task_hello = BashOperator(
        task_id='hello_world',
        bash_command='echo "Hello from Airflow!"'
    )

    task_list_files = BashOperator(
        task_id='list_files',
        bash_command='ls -l /opt/airflow/dags'
    )

    task_check_pip_list = BashOperator(
        task_id='pip_list',
        bash_command='python -m pip list'
    )

    # Thiết lập thứ tự chạy
    task_hello >> task_list_files >> task_check_pip_list

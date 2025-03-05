FROM apache/airflow:latest

WORKDIR /opt/airflow/

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

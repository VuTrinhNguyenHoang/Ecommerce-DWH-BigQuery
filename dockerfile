FROM apache/airflow:latest

USER root
RUN apt-get update && apt-get install -y cmake build-essential
USER airflow

WORKDIR /opt/airflow/

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

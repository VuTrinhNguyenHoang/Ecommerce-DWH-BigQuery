FROM apache/airflow:latest

USER root
RUN apt-get update && apt-get install -y git build-essential libkrb5-dev && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow

WORKDIR /opt/airflow/

RUN pip install pandas pyarrow hdfs

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

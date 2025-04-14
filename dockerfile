FROM apache/airflow:latest

USER root

RUN apt-get update && \
    apt-get install -y \
        cmake \
        build-essential \
        libkrb5-dev \
        krb5-user && \  
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN apt-get update && apt-get install -y cmake build-essential
USER airflow

WORKDIR /opt/airflow/

RUN pip install pandas pyarrow hdfs

RUN pip install apache-airflow-providers-apache-spark

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

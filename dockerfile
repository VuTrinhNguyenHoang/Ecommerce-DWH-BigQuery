FROM apache/airflow:latest-python3.10

USER root
RUN apt-get update && apt-get install -y bash openjdk-17-jre-headless
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:$PATH"
USER airflow

WORKDIR /opt/airflow/

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

FROM apache/airflow:2.10.5-python3.10

USER root
RUN apt-get update && apt-get install -y bash openjdk-17-jre-headless
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:$PATH"
USER airflow

COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt 

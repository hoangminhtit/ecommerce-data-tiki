FROM apache/airflow:2.3.0-python3.8

USER root
# RUN apt-get update && apt-get install -y bash openjdk-17-jre-headless
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# ENV PATH="${JAVA_HOME}/bin:$PATH"
USER airflow

COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt 

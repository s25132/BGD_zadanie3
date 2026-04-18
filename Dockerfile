FROM apache/airflow:3.1.0-python3.11

USER root

RUN apt-get update && apt-get install -y \
    default-jdk \
    procps \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PYTHONPATH=/opt/airflow:/opt/airflow/src
ENV PYTHONUNBUFFERED=1

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
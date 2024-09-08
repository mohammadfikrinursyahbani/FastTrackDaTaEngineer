FROM apache/airflow:2.10.0

USER root

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    openjdk-17-jdk \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt /requirements.txt

USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark \
  && pip install --no-cache-dir pyspark==3.5.2 \
  && pip install --no-cache-dir psycopg2-binary==2.9.9 \
  && pip install --no-cache-dir mysql-connector-python==9.0.0 \
  && pip install --no-cache-dir polars==1.6.0

FROM apache/airflow:2.9.2

ENV AIRFLOW_HOME=/opt/airflow

USER root
# Install necessary packages
RUN apt-get update && apt-get clean

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Setup SHELL interaction
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME
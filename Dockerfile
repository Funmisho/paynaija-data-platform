FROM apache/airflow:2.9.1-python3.10

COPY --chown=airflow:root requirements.txt /requirements.txt

ARG AIRFLOW_VERSION=2.9.1

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
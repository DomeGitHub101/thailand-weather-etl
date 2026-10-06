ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.3.1
FROM ${AIRFLOW_BASE_IMAGE}

COPY requirements-airflow.txt /requirements-airflow.txt

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements-airflow.txt \
    --constraint "${HOME}/constraints.txt"
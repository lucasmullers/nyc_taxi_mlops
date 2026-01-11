FROM apache/airflow:2.11.0

COPY requirements.txt  ./

USER airflow

RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} && \
    pip install --no-cache-dir -r requirements.txt
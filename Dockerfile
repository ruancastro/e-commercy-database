FROM apache/airflow:2.10.5
USER airflow
RUN pip install --no-cache-dir sqlalchemy psycopg2-binary faker
USER root
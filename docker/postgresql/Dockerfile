FROM postgres:latest
COPY init_main.sql /docker-entrypoint-initdb.d/init_main.sql
COPY init_airflow.sql /docker-entrypoint-initdb.d/init_airflow.sql
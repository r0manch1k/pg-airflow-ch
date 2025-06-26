-- TODO: Add variables from .env
CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

ALTER USER airflow SET search_path = public;

ALTER DATABASE airflow OWNER TO airflow;

-- PostgreSQL 15 requires additional privileges:
-- Note: Connect to the airflow_db database before running the following GRANT statement
-- You can do this in psql with: \c airflow_db
-- GRANT ALL ON SCHEMA public TO airflow;
#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
    CREATE DATABASE IF NOT EXISTS main;
    CREATE USER IF NOT EXISTS airflow IDENTIFIED WITH plaintext_password BY 'airflow';
    GRANT SELECT, INSERT, ALTER, CREATE, DROP, SHOW, UPDATE, DELETE ON main.* TO airflow;
EOSQL

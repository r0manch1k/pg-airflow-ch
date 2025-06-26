# pg-airflow-ch

Simple example of migrating data from PostgreSQL to Clickhouse using Apache Airflow built with Docker

# Getting Stated

Copy `.env` file to `.env.local`

```
cp .env .env.local
```

Build and up containers

```sh
make up
# make up-logs - So verbose. Not recommended.
```

Go to `http://localhost:8080`

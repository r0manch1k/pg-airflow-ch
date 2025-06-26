-include .env.local
export

COMPOSE = docker-compose -f ./docker-compose.yml --env-file .env.local

# CLEANING (USE WITH CAUTION)

.PHONY: rm-volumes
rm-volumes:
	$(COMPOSE) down -v
	docker volume prune -a -f

.PHONY: rm-images
rm-images:
	$(COMPOSE) down --rmi all
	docker image prune -a -f

.PHONY: rm-containers
rm-containers:
	docker container prune -f

.PHONY: rm-networks
rm-networks:
	docker network prune -f

.PHONY: rm-system
rm-system: 
	docker system prune --volumes -f

.PHONY: rm-all
rm-all: rm-volumes rm-images rm-containers rm-system rm-networks

# DOCKER

.PHONY: build
build:
	$(COMPOSE) build

.PHONY: up
up:
	$(COMPOSE) --profile flower up -d
.PHONY: up-logs
up-logs:
	$(COMPOSE) --profile flower up

# WORKSPACE

.PHONY: rm-all-data
rm-all-data:
	rm -rf ./etc/postgresql/data/* && \
	rm -rf ./etc/clickhouse-server/data/* && \
	rm -rf ./etc/airflow/* && \
	rm -rf ./var/log/postgresql/* && \
	rm -rf ./var/log/clickhouse-server/* && \
	rm -rf ./var/log/airflow/*

# AIRFLOW

.PHONY: dags-list
dags-list:
	$(COMPOSE) run --rm airflow-cli dags list

.PHONY: scheduler
scheduler:
	$(COMPOSE) run --rm airflow-cli scheduler

.PHONY: delete-dag
delete-dag:
	$(COMPOSE) run -T --rm airflow-cli dags delete $(DAG_ID) -y
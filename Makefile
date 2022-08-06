#!make
APP_HOST ?= 0.0.0.0
APP_PORT ?= 8080
EXTERNAL_APP_PORT ?= ${APP_PORT}

run_app = docker-compose run --rm \
				-p ${EXTERNAL_APP_PORT}:${APP_PORT} \
				-e APP_HOST=${APP_HOST} \
				-e APP_PORT=${APP_PORT} \
				app

.PHONY: image
image:
	docker-compose build

.PHONY: docker-run-all
docker-run-all:
	docker-compose up

.PHONY: docker-run-app
docker-run-app: image
	$(run_app)

.PHONY: docker-shell
docker-shell:
	$(run_app) /bin/bash

.PHONY: test
test:
	$(run_app) /bin/bash -c 'export && ./scripts/wait-for-it.sh database:5432 && cd /app/tests/ && pytest -vvv'

.PHONY: run-database
run-database:
	docker-compose run --rm database

.PHONY: run-joplin
run-joplin:
	docker-compose run --rm loadjoplin

.PHONY: install
pgstac-install:
	pip install -e .[dev,server]

.PHONY: docs-image
docs-image:
	docker-compose -f docker-compose.docs.yml \
		build

.PHONY: docs
docs: docs-image
	docker-compose -f docker-compose.docs.yml \
		run docs

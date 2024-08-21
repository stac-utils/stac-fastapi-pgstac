#!make
APP_HOST ?= 0.0.0.0
APP_PORT ?= 8080
EXTERNAL_APP_PORT ?= ${APP_PORT}
LOG_LEVEL ?= warning

run = docker compose run --rm \
				-p ${EXTERNAL_APP_PORT}:${APP_PORT} \
				-e APP_HOST=${APP_HOST} \
				-e APP_PORT=${APP_PORT} \
				app

runtests = docker compose run --rm tests

.PHONY: image
image:
	docker compose build

.PHONY: docker-run
docker-run: image
	docker compose up

.PHONY: docker-run-nginx-proxy
docker-run-nginx-proxy:
	docker compose -f docker-compose.yml -f docker-compose.nginx.yml up

.PHONY: docker-shell
docker-shell:
	$(run) /bin/bash

.PHONY: test
test:
	$(runtests) /bin/bash -c 'export && python -m pytest /app/tests/api/test_api.py --log-cli-level $(LOG_LEVEL)'

.PHONY: run-database
run-database:
	docker compose run --rm database

.PHONY: run-joplin
run-joplin:
	docker compose run --rm loadjoplin

.PHONY: install
install:
	pip install -e .[dev,server]

.PHONY: docs-image
docs-image:
	docker compose -f docker-compose.docs.yml \
		build

.PHONY: docs
docs: docs-image
	docker compose -f docker-compose.docs.yml \
		run docs

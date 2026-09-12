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

runtests = docker compose -f compose.yml -f compose.tests.yml run --rm tests

.PHONY: image
image:
	docker compose build

.PHONY: image-tests
image-tests:
	docker compose -f compose.yml -f compose.tests.yml build

.PHONY: docker-run
docker-run: image
	docker compose up

.PHONY: docker-run-nginx-proxy
docker-run-nginx-proxy: image
	docker compose -f compose.yml -f compose.nginx.yml up

.PHONY: docker-down
docker-down:
	docker compose down

.PHONY: docker-down-nginx
docker-down-nginx:
	docker compose -f compose.yml -f compose.nginx.yml down

.PHONY: docker-down-tests
docker-down-tests:
	docker compose -f compose.yml -f compose.tests.yml down

.PHONY: docker-down-all
docker-down-all:
	docker compose down
	docker compose -f compose.yml -f compose.nginx.yml down
	docker compose -f compose.yml -f compose.tests.yml down

.PHONY: docker-shell
docker-shell:
	$(run) /bin/bash

.PHONY: test
test:
	$(runtests) /bin/bash -c 'export && python -m pytest /app/tests/ --log-cli-level $(LOG_LEVEL)'

.PHONY: test-catalogs
test-catalogs:
	$(runtests) python -m pytest /app/tests/extensions/test_catalogs.py -v --log-cli-level $(LOG_LEVEL)

.PHONY: run-database
run-database:
	docker compose run --rm database

.PHONY: load-joplin
load-joplin:
	python scripts/ingest_joplin.py http://localhost:8082

.PHONY: install
install:
	uv sync --dev

.PHONY: pytest
pytest: install
	uv run pytest

.PHONY: docs
docs:
	uv run --group docs mkdocs build -f docs/mkdocs.yml
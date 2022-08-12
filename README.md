<p align="center">
  <img src="https://github.com/radiantearth/stac-site/raw/master/images/logo/stac-030-long.png" width=400>
  <p align="center">FastAPI implemention of the STAC API spec using <a href="https://github.com/stac-utils/pgstac">PgSTAC</a>.</p>
</p>
<p align="center">
  <a href="https://github.com/stac-utils/stac-fastapi-pgstac/actions?query=workflow%3Acicd" target="_blank">
      <img src="https://github.com/stac-utils/stac-fastapi-pgstac/workflows/stac-fastapi-pgstac/badge.svg" alt="Test">
  </a>
  <a href="https://pypi.org/project/stac-fastapi-pgstac" target="_blank">
      <img src="https://img.shields.io/pypi/v/stac-fastapi-pgstac?color=34D058&label=pypi%20package" alt="Package version">
  </a>
  <a href="https://github.com/stac-utils/stac-fastapi-pgstac/blob/master/LICENSE" target="_blank">
      <img src="https://img.shields.io/pypi/l/stac-fastapi-pgstac" alt="License">
  </a>
</p>

---

**Documentation**: [https://stac-utils.github.io/stac-fastapi-pgstac/](https://stac-utils.github.io/stac-fastapi-pgstac/)

**Source Code**: [https://github.com/stac-utils/stac-fastapi](https://github.com/stac-utils/stac-fastapi-pgstac)

---

PostgreSQL/PostGIS backend implementation for the [stac-fastapi](https://github.com/stac-utils/stac-fastapi) library.

`stac-fastapi` was initially developed by [arturo-ai](https://github.com/arturo-ai).

## Installation

```bash
# Install from pypi.org
pip install stac-fastapi.pgstac

#/////////////////////
# Install from source

git clone https://github.com/stac-utils/stac-fastapi-pgstac.git && cd stac-fastapi-pgstac
pip install -e .
```

## Local Development

Use docker-compose via make to start the application, migrate the database, and ingest some example data:
```bash
make image
make docker-run-all
```

- The app will be available on <http://localhost:8082>.

You can also launch the application without ingesting the `joplin` example data:

```shell
make docker-run-app
```

The application will be started on <http://localhost:8080>.

By default, the apps are run with uvicorn hot-reloading enabled. This can be turned off by changing the value
of the `RELOAD` env var in docker-compose.yml to `false`.

#### Note to Docker for Windows users

You'll need to enable experimental features on Docker for Windows in order to run the docker-compose,
due to the "--platform" flag that is required to allow the project to run on some Apple architectures.
To do this, open Docker Desktop, go to settings, select "Docker Engine", and modify the configuration
JSON to have `"experimental": true`.

### Testing

Before running the tests, ensure the database and apps run with docker-compose are down:

```shell
docker-compose down
```

To run tests:

```shell
make test
```

Run individual tests by running pytest within a docker container:

```shell
make docker-shell
$ pip install -e .[dev]
$ pytest -v tests/api/test_api.py 
```

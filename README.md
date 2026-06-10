# stac-fastapi-pgstac

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/stac-utils/stac-fastapi-pgstac/cicd.yaml?style=for-the-badge)](https://github.com/stac-utils/stac-fastapi-pgstac/actions/workflows/cicd.yaml)
[![PyPI](https://img.shields.io/pypi/v/stac-fastapi.pgstac?style=for-the-badge)](https://pypi.org/project/stac-fastapi.pgstac)
[![Documentation](https://img.shields.io/github/actions/workflow/status/stac-utils/stac-fastapi-pgstac/pages.yml?label=Docs&style=for-the-badge)](https://stac-utils.github.io/stac-fastapi-pgstac/)
[![License](https://img.shields.io/github/license/stac-utils/stac-fastapi-pgstac?style=for-the-badge)](https://github.com/stac-utils/stac-fastapi-pgstac/blob/main/LICENSE)

<p align="center">
  <img src="https://user-images.githubusercontent.com/10407788/174893876-7a3b5b7a-95a5-48c4-9ff2-cc408f1b6af9.png" style="vertical-align: middle; max-width: 400px; max-height: 100px;" height=100 />
  <img src="https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png" alt="FastAPI" style="vertical-align: middle; max-width: 400px; max-height: 100px;" width=200 />
</p>

[PgSTAC](https://github.com/stac-utils/pgstac) backend for [stac-fastapi](https://github.com/stac-utils/stac-fastapi), the [FastAPI](https://fastapi.tiangolo.com/) implementation of the [STAC API spec](https://github.com/radiantearth/stac-api-spec)

## Overview

**stac-fastapi-pgstac** is an HTTP interface built in FastAPI.
It validates requests and data sent to a [PgSTAC](https://github.com/stac-utils/pgstac) backend, and adds [links](https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#link-object) to the returned data.
All other processing and search is provided directly using PgSTAC procedural sql / plpgsql functions on the database.
PgSTAC stores all collection and item records as jsonb fields exactly as they come in allowing for any custom fields to be stored and retrieved transparently.

## `PgSTAC` version

`stac-fastapi-pgstac` depends on [`pgstac`](https://stac-utils.github.io/pgstac/pgstac/) database schema and [`pypgstac`](https://stac-utils.github.io/pgstac/pypgstac/) python package.

| stac-fastapi-pgstac Version  |     pgstac |
|                            --|          --|
|                          2.5 | >=0.7,<0.8 |
|                          3.0 | >=0.8,<0.9 |
|                        >=4.0 | >=0.8,<0.10|

## Usage

PgSTAC is an external project and may be used by multiple front ends.
For STAC FastAPI development, a Docker image (which is pulled as part of the docker-compose) is available via the [Github container registry](https://github.com/stac-utils/pgstac/pkgs/container/pgstac/81689794?tag=latest).
The PgSTAC version required by **stac-fastapi-pgstac** is found in the [setup](http://github.com/stac-utils/stac-fastapi-pgstac/blob/main/setup.py) file.

### Sorting

While the STAC [Sort Extension](https://github.com/stac-api-extensions/sort) is fully supported, [PgSTAC](https://github.com/stac-utils/pgstac) is particularly enhanced to be able to sort by datetime (either ascending or descending).
Sorting by anything other than datetime (the default if no sort is specified) on very large STAC repositories without very specific query limits (ie selecting a single day date range) will not have the same performance.
For more than millions of records it is recommended to either set a low connection timeout on PostgreSQL or to disable use of the Sort Extension.

### Hydration

To configure **stac-fastapi-pgstac** to [hydrate search result items at the API level](https://stac-utils.github.io/pgstac/pgstac/#runtime-configurations), set the `USE_API_HYDRATE` environment variable to `true`. If `false` (default) the hydration will be done in the database.

| use_api_hydrate (API) | nohydrate (PgSTAC) | Hydration |
|                  --- |                --- |       --- |
|                False |              False |    PgSTAC |
|                 True |               True |       API |

### Multi-Tenant Catalogs Extension

**stac-fastapi-pgstac** supports the optional [Multi-Tenant Catalogs Extension](https://github.com/StacLabs/multi-tenant-catalogs) for managing hierarchical catalog structures with support for Directed Acyclic Graphs (DAG).
This enables flexible catalog hierarchies where collections and catalogs can have multiple parents.

To enable this extension, install the `stac-fastapi-catalogs-extension` package and set the `ENABLE_CATALOGS_EXTENSION=TRUE` environment variable.

For write operations (creating, updating, and deleting catalogs, and linking/unlinking collections and catalogs), also set `ENABLE_TRANSACTIONS_EXTENSIONS=TRUE`.

#### Poly-Hierarchy Links

When a catalog or collection has multiple parents, the API exposes the catalog hierarchy through STAC link relations:

- `rel="parent"`: Points to the contextual parent (the catalog through which the resource was accessed)
- `rel="related"`: Links to alternative parents in the poly-hierarchy (for catalogs and scoped collections)
- `rel="duplicate"`: Links to alternative scoped paths where a collection can be accessed (e.g., `/catalogs/{parentId}/collections/{collectionId}`)
- `rel="canonical"`: Points to the global collection endpoint for scoped collections

To prevent information leakage about other tenants in multi-tenant deployments, set `HIDE_ALTERNATE_PARENTS=TRUE` to suppress `rel="related"` and `rel="duplicate"` links. When enabled, only the contextual `rel="parent"` link is advertised.

**Note:** The link relation names for poly-hierarchy navigation are subject to change as the OGC and STAC communities continue to standardize on terminology. These names may be updated in future releases to align with emerging standards.

### Migrations

There is a Python utility as part of PgSTAC ([pypgstac](https://stac-utils.github.io/pgstac/pypgstac/)) that includes a migration utility.
To use:

```shell
pypgstac migrate
```

## Development

### Quick Start

Install the packages in editable mode:

We recommend using [`uv`](https://docs.astral.sh/uv) as project manager for development.

See https://docs.astral.sh/uv/getting-started/installation/ for installation

```shell
uv sync --dev
```

### Running the API Locally

Start the API with Docker Compose:

```shell
make docker-run
```

The API will be available at `http://localhost:8082`

### Running with Nginx Proxy

To run the API behind an Nginx proxy:

```shell
make docker-run-nginx-proxy
```

The API will be available at:
- Direct: `http://localhost:8082`
- Via Nginx: `http://localhost:8080/api/v1/pgstac/`

### Loading Demo Data

To load the Joplin demo dataset:

```shell
make load-joplin
```

### Running Tests

To run tests locally (requires postgres/postgis system packages):

```shell
uv run pytest
```

**NOTE:** If running tests directly on your machine doesn't work, you can use Docker Compose instead. You need [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) installed.

To run tests in a container:

```shell
make test
```

### Stopping Services

To stop running services:

```shell
make docker-down          # Stop the default app
make docker-down-nginx    # Stop the nginx variant
make docker-down-all      # Stop all services
```

## Contributing

See [CONTRIBUTING](./contributing.md) for detailed contribution instructions.

## Releasing

See [RELEASING.md](./releasing.md).

## History

**stac-fastapi-pgstac** was initially added to **stac-fastapi** by [developmentseed](https://github.com/developmentseed).
In April of 2023, it was removed from the core **stac-fastapi** repository and moved to its current location (<http://github.com/stac-utils/stac-fastapi-pgstac>).

## License

[MIT](https://github.com/stac-utils/stac-fastapi-pgstac/blob/main/LICENSE)

<!-- markdownlint-disable-file MD033 -->

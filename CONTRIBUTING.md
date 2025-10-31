# Contributing

Use Github [Pull Requests](https://github.com/stac-utils/stac-fastapi-pgstac/pulls) to provide new features or to request review of draft code, and use [Issues](https://github.com/stac-utils/stac-fastapi-pgstac/issues) to report bugs or request new features.

## Development install

We recommand using [`uv`](https://docs.astral.sh/uv) as project manager for development.

See https://docs.astral.sh/uv/getting-started/installation/ for installation 

**dev install**

```bash
git clone https://github.com/stac-utils/stac-fastapi-pgstac.git
cd stac-fastapi
uv sync --dev
```

To run the service on 0.0.0.0:8082 and ingest example data into the database (the "joplin" collection):

```shell
make run-joplin
```

You can connect to the database with a database tool on port 5439 to inspect and see the data. 

To run the tests:

```shell
make test
```

**pre-commit**

This repo is set to use `pre-commit` to run *isort*, *flake8*, *pydocstring*, *black* ("uncompromising Python code formatter") and mypy when committing new code.

```shell
pre-commit install
```

### Docs

```bash
git clone https://github.com/stac-utils/stac-fastapi-pgstac.git
cd stac-fastapi-pgstac
# Build docs
uv run --group docs mkdocs build -f docs/mkdocs.yml
```

Hot-reloading docs:

```bash
uv run --group docs mkdocs serve -f docs/mkdocs.yml --livereload
```

To manually deploy docs (note you should never need to do this because GitHub
Actions deploys automatically for new commits.):

```bash
# deploy
uv run --group docs mkdocs gh-deploy -f docs/mkdocs.yml
```

# Contributing

Issues and pull requests are more than welcome.

## Development install

```shell
git clone https://github.com/stac-utils/stac-fastapi-pgstac
cd stac-fastapi-pgstac
make install
```

This repo is set to use `pre-commit` to run *isort*, *flake8*, *pydocstring*, *black* ("uncompromising Python code formatter") and mypy when committing new code.

```shell
pre-commit install
```

## Docs

```bash
git clone https://github.com/stac-utils/stac-fastapi-pgstac
cd stac-fastapi-pgstac
pip install -e .[docs]
```

Hot-reloading docs:

```bash
mkdocs serve
```

To manually deploy docs (note you should never need to do this because GitHub
Actions deploys automatically for new commits.):

```shell
# Create API documentations
make docs
# deploy
mkdocs gh-deploy
```

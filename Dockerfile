ARG PYTHON_VERSION=3.11

FROM python:${PYTHON_VERSION}-slim as base

# Any python libraries that require system libraries to be installed will likely
# need the following packages in order to build
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y build-essential git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

FROM base as builder

WORKDIR /app

COPY . /app

COPY README.md README.md
COPY LICENSE LICENSE
COPY stac_fastapi/ stac_fastapi/
COPY pyproject.toml pyproject.toml
COPY setup.cfg setup.cfg
COPY setup.py setup.py
COPY VERSION VERSION

RUN python -m pip install -e .[server] httpx
RUN rm -rf README.md LICENSE stac_fastapi/ pyproject.toml setup.cfg setup.py VERSION

# http://www.uvicorn.org/settings/
ENV APP_HOST 0.0.0.0
ENV APP_PORT 80
CMD uvicorn stac_fastapi.pgstac.app:app --host ${APP_HOST} --port ${APP_PORT}

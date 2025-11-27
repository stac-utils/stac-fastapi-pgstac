ARG PYTHON_VERSION=3.14

FROM python:${PYTHON_VERSION}-slim AS base

# Any python libraries that require system libraries to be installed will likely
# need the following packages in order to build
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y build-essential git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

FROM base AS builder

RUN python -m pip install -U pip

WORKDIR /app

COPY stac_fastapi/ stac_fastapi/
COPY scripts/ scripts/
COPY pyproject.toml pyproject.toml
COPY README.md README.md

RUN python -m pip install .[server]
RUN rm -rf stac_fastapi .toml README.md

RUN groupadd -g 1000 user && \
    useradd -u 1000 -g user -s /bin/bash -m user
USER user

CMD ["uvicorn", "stac_fastapi.pgstac.app:app", "--host", "0.0.0.0", "--port", "8080"]

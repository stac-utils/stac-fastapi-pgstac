ARG PYTHON_VERSION=3.11

FROM python:${PYTHON_VERSION}-slim as base

# Any python libraries that require system libraries to be installed will likely
# need the following packages in order to build
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y build-essential git libpq-dev postgresql-15-postgis-3  && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

FROM base as builder

RUN useradd -ms /bin/bash newuser
USER newuser

WORKDIR /app
COPY . /app

RUN python -m pip install -e .[dev] --user

CMD ["uvicorn", "stac_fastapi.pgstac.app:app", "--host", "0.0.0.0", "--port", "8080"]

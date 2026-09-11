ARG PYTHON_VERSION=3.14

FROM python:${PYTHON_VERSION}  AS builder

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install build dependencies
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y build-essential git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Configure uv-managed virtual environment
ENV UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:${PATH}"

WORKDIR /tmp

# Copy project metadata and install dependencies
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --frozen --no-dev --extra server --extra catalogs --no-install-project

# Copy project code and install it in the virtual environment
COPY stac_fastapi/ ./stac_fastapi/
RUN uv pip install --no-deps --no-editable .

# Runtime stage
FROM python:${PYTHON_VERSION}-slim

LABEL org.opencontainers.image.source="https://github.com/stac-utils/stac-fastapi-pgstac"
LABEL org.opencontainers.image.description="STAC FastAPI PGStac"
LABEL org.opencontainers.image.licenses="MIT"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH"

# Install build dependencies
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y build-essential git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

RUN groupadd -g 1000 user && \
    useradd -u 1000 -g user -s /bin/bash -m user
USER user

CMD ["uvicorn", "stac_fastapi.pgstac.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8080"]

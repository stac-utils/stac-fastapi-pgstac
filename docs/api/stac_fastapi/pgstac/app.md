# stac_fastapi.pgstac.app

## Overview

The `stac_fastapi.pgstac.app` module contains the main application configuration for the FastAPI-based STAC API that uses PgSTAC as the backend. This module defines how the application is constructed, which extensions are enabled, and how the API endpoints are registered.

## Key Components

### PgStacApi Class

```python
class PgStacApi(StacApi)
```

Extended version of the `StacApi` class that provides PgSTAC-specific functionality.

#### Methods

##### add_health_check

```python
def add_health_check(self)
```

Adds a health check endpoint at `/_mgmt/ping` that verifies database connectivity.

- The endpoint attempts to establish a database connection using the application's read connection pool.
- It verifies PgSTAC is properly set up by querying the `pgstac.migrations` table.
- Returns:
  - Status 200 with `{"message": "PONG", "database": "OK"}` when the database is healthy.
  - Status 503 with error details when the database cannot be reached or when PgSTAC is not properly set up.

### Application Creation

The module defines several key components for the FastAPI application:

1. **Settings**: Configuration settings for the application.
2. **Extensions**: Various STAC API extensions that are enabled.
3. **Search Models**: Request/response models for search endpoints.
4. **Database Connection**: Configuration for connecting to PostgreSQL with PgSTAC.

### Lifespan

```python
@asynccontextmanager
async def lifespan(app: FastAPI)
```

Manages the application lifespan:
- Connects to the database during startup
- Closes database connections during shutdown

## Usage

The module creates a FastAPI application with a PgSTAC backend:

```python
api = PgStacApi(
    app=FastAPI(...),
    settings=settings,
    extensions=application_extensions,
    client=CoreCrudClient(pgstac_search_model=post_request_model),
    ...
)
app = api.app
```

The application can be run directly with uvicorn or used as a Lambda handler.

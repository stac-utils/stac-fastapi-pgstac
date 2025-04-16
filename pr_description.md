# Enhance /_mgmt/ping endpoint to check if pgstac is ready

## Overview
This PR enhances the `/_mgmt/ping` health check endpoint to actually verify database connectivity before returning a positive response. The current implementation always returns `{"message": "PONG"}` regardless of whether the database is actually accessible, which can lead to misleading health checks in production environments.

## Changes Made
- Created a `PgStacApi` class in `stac_fastapi/pgstac/app.py` that extends the base `StacApi` class
- Overrode the `add_health_check` method to implement database connectivity checks
- Modified the ping endpoint to:
  - Test database connectivity by attempting to connect to the read pool
  - Verify pgstac is properly set up by querying the `pgstac.migrations` table
  - Return appropriate status codes and error messages when the database is unavailable
- Updated the test in `tests/resources/test_mgmt.py` to verify the new response format

## Implementation Details
The enhanced endpoint now:
- Returns `{"message": "PONG", "database": "OK"}` with status 200 when the database is healthy
- Returns a 503 Service Unavailable with descriptive error message when the database cannot be reached or pgstac is not properly set up

## Why This Is Important
- Provides more accurate health/readiness checks in containerized environments
- Better integration with container orchestration systems like Kubernetes
- Faster detection of database connectivity issues
- Helps operators quickly identify when database connectivity is the root cause of issues

## Testing
The implementation can be tested by:

1. Running the API with a working database connection:
```bash
# Start with a working database
docker-compose up -d
# The endpoint should return status 200
curl -v http://localhost:8080/_mgmt/ping
```

2. Testing with a non-functioning database:
```bash
# Stop the database
docker-compose stop pgstac
# The endpoint should now return a 503 error
curl -v http://localhost:8080/_mgmt/ping
```

## Alternatives Considered
I considered two alternative approaches:

1. **Using a simple connection check without querying any tables** - This would only verify the database is running but not that pgstac is properly set up.

2. **Implementing a more extensive set of health checks** - While more comprehensive, this would add complexity and potential performance overhead to the health check endpoint.

The current implementation provides a good balance between thoroughness and simplicity.

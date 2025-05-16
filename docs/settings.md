

### Application Extension

The default `stac-fastapi-pgstac` application comes will **all** extensions enabled (except transaction). Users can use `ENABLED_EXTENSIONS` environment variable to limit the supported extensions.

Available values for `ENABLED_EXTENSIONS`:

- `query`
- `sort`
- `fields`
- `filter`
- `free_text` (only for collection-search)
- `pagination`
- `collection_search`

Example: `ENABLED_EXTENSIONS="pagination,sort"`


Since `6.0.0`, the transaction extension is not enabled by default. To add the transaction endpoints, users can set `ENABLE_TRANSACTIONS_EXTENSIONS=TRUE/YES/1`.

### Database config

- `PGUSER`: postgres username
- `PGPASSWORD`: postgres password
- `PGHOST`: hostname for the connection
- `PGPORT`: database port
- `PGDATABASE`: database name
- `DB_MIN_CONN_SIZE`: Number of connection the pool will be initialized with. Defaults to `1`
- `DB_MAX_CONN_SIZE` Max number of connections in the pool. Defaults to `10`
- `DB_MAX_QUERIES`: Number of queries after a connection is closed and replaced with a new connection. Defaults to `50000`
- `DB_MAX_INACTIVE_CONN_LIFETIME`: Number of seconds after which inactive connections in the pool will be closed. Defaults to `300`
- `SEARCH_PATH`: Postgres search path. Defaults to `"pgstac,public"`
- `APPLICATION_NAME`: PgSTAC Application name. Defaults to `"pgstac"`

##### Deprecated

In version `6.0.0` we've renamed the PG configuration variable to match the official naming convention:

- `POSTGRES_USER` -> `PGUSER`
- `POSTGRES_PASS` -> `PGPASSWORD`
- `POSTGRES_HOST_READER` -> `PGHOST`
- `POSTGRES_HOST_WRITER` -> `PGHOST`*
- `POSTGRES_PORT` -> `PGPORT`
- `POSTGRES_DBNAME` -> `PGDATABASE`

\* Since version `6.0`, users cannot set a different host for `writer` and `reader` database but will need to customize the application and pass a specific `stac_fastapi.pgstac.config.PostgresSettings` instance to the `connect_to_db` function.

### Validation/Serialization

- `ENABLE_RESPONSE_MODELS`: use pydantic models to validate endpoint responses. Defaults to `False`
- `ENABLE_DIRECT_RESPONSE`: by-pass the default FastAPI serialization by wrapping the endpoint responses into `starlette.Response` classes. Defaults to `False`

### Misc

- `STAC_FASTAPI_VERSION` (string) is the version number of your API instance (this is not the STAC version)
- `STAC FASTAPI_TITLE` (string) should be a self-explanatory title for your API
- `STAC FASTAPI_DESCRIPTION` (string) should be a good description for your API. It can contain CommonMark
- `STAC_FASTAPI_LANDING_ID` (string) is a unique identifier for your Landing page
- `ROOT_PATH`: set application root-path (when using proxy)
- `CORS_ORIGINS`: A list of origins that should be permitted to make cross-origin requests. Defaults to `*`
- `CORS_METHODS`: A list of HTTP methods that should be allowed for cross-origin requests. Defaults to `"GET,POST,OPTIONS"`
- `USE_API_HYDRATE`: perform hydration of stac items within stac-fastapi
- `INVALID_ID_CHARS`: list of characters that are not allowed in item or collection ids (used in Transaction endpoints)

#!/bin/bash
# Load Joplin demo dataset into the STAC API

set -e

# Default to regular app
APP_HOST=${APP_HOST:-"http://localhost:8082"}
MAX_RETRIES=60
RETRY_DELAY=1

echo "Waiting for API to be ready at $APP_HOST..."

# Wait for the API to be ready
for i in $(seq 1 $MAX_RETRIES); do
    if curl -s "$APP_HOST" > /dev/null 2>&1; then
        echo "API is ready!"
        break
    fi
    if [ $i -eq $MAX_RETRIES ]; then
        echo "API did not become ready after ${MAX_RETRIES} seconds"
        exit 1
    fi
    echo "Attempt $i/$MAX_RETRIES: Waiting for API..."
    sleep $RETRY_DELAY
done

echo "Installing dependencies..."
python -m pip install -q pip -U
python -m pip install -q requests

echo "Loading Joplin Collection..."
python /app/scripts/ingest_joplin.py "$APP_HOST"

echo "All Done! Joplin data loaded successfully."

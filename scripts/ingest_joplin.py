"""Ingest sample data during docker-compose"""
import json
import sys
from pathlib import Path
from urllib.parse import urljoin

import httpx

workingdir = Path(__file__).parent.absolute()
joplindata = workingdir.parent / "testdata" / "joplin"

app_host = sys.argv[1]

if not app_host:
    raise Exception("You must include full path/port to stac instance")


def post_or_put(url: str, data: dict):
    """Post or put data to url."""
    r = httpx.post(url, json=data)
    if r.status_code == 409:
        new_url = url if data["type"] == "Collection" else url + f"/{data['id']}"
        # Exists, so update
        r = httpx.put(new_url, json=data)
        # Unchanged may throw a 404
        if not r.status_code == 404:
            r.raise_for_status()
    else:
        r.raise_for_status()


def ingest_joplin_data(app_host: str = app_host, data_dir: Path = joplindata):
    """ingest data."""

    with open(data_dir / "collection.json") as f:
        collection = json.load(f)

    post_or_put(urljoin(app_host, "/collections"), collection)

    with open(data_dir / "index.geojson") as f:
        index = json.load(f)

    for feat in index["features"]:
        post_or_put(urljoin(app_host, f"collections/{collection['id']}/items"), feat)


if __name__ == "__main__":
    ingest_joplin_data()

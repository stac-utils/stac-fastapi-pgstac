"""link helpers."""

from typing import Any, Dict, List, Optional
from urllib.parse import ParseResult, parse_qs, unquote, urlencode, urljoin, urlparse

import attr
from stac_fastapi.types.requests import get_base_url
from stac_pydantic.links import Relations
from stac_pydantic.shared import MimeTypes
from starlette.requests import Request

# These can be inferred from the item/collection so they aren't included in the database
# Instead they are dynamically generated when querying the database using the classes defined below
INFERRED_LINK_RELS = ["self", "item", "parent", "collection", "root"]


def filter_links(links: List[Dict]) -> List[Dict]:
    """Remove inferred links."""
    return [link for link in links if link["rel"] not in INFERRED_LINK_RELS]


def merge_params(url: str, newparams: Dict) -> str:
    """Merge url parameters."""
    u = urlparse(url)
    params = parse_qs(u.query)
    params.update(newparams)
    param_string = unquote(urlencode(params, True))

    href = ParseResult(
        scheme=u.scheme,
        netloc=u.netloc,
        path=u.path,
        params=u.params,
        query=param_string,
        fragment=u.fragment,
    ).geturl()
    return href


@attr.s
class BaseLinks:
    """Create inferred links common to collections and items."""

    request: Request = attr.ib()

    @property
    def base_url(self):
        """Get the base url."""
        return get_base_url(self.request)

    @property
    def url(self):
        """Get the current request url."""
        base_url = self.request.base_url
        path = self.request.url.path

        # root path can be set in the request scope in two different ways:
        # - by uvicorn when running with --root-path
        # - by FastAPI when running with FastAPI(root_path="...")
        #
        # We need to remove the root path prefix from the path before
        # joining the base_url and path to get the full url to avoid
        # having root_path twice in the url
        if root_path := self.request.scope.get("root_path"):
            if path.startswith(root_path):
                path = path[len(root_path) :]

        url = urljoin(str(base_url), path.lstrip("/"))
        if qs := self.request.url.query:
            url += f"?{qs}"

        return url

    def resolve(self, url):
        """Resolve url to the current request url."""
        return urljoin(str(self.base_url), str(url))

    def link_self(self) -> Dict:
        """Return the self link."""
        return {
            "rel": Relations.self.value,
            "type": MimeTypes.json.value,
            "href": self.url,
        }

    def link_root(self) -> Dict:
        """Return the catalog root."""
        return {
            "rel": Relations.root.value,
            "type": MimeTypes.json.value,
            "href": self.base_url,
        }

    def create_links(self) -> List[Dict[str, Any]]:
        """Return all inferred links."""
        links = []
        for name in dir(self):
            if name.startswith("link_") and callable(getattr(self, name)):
                link = getattr(self, name)()
                if link is not None:
                    links.append(link)
        return links

    async def get_links(
        self, extra_links: Optional[List[Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        """
        Generate all the links.

        Get the links object for a stac resource by iterating through
        available methods on this class that start with link_.
        """
        # TODO: Pass request.json() into function so this doesn't need to be coroutine
        if self.request.method == "POST":
            self.request.postbody = await self.request.json()
        # join passed in links with generated links
        # and update relative paths
        links = self.create_links()

        if extra_links:
            # For extra links passed in,
            # add links modified with a resolved href.
            # Drop any links that are dynamically
            # determined by the server (e.g. self, parent, etc.)
            # Resolving the href allows for relative paths
            # to be stored in pgstac and for the hrefs in the
            # links of response STAC objects to be resolved
            # to the request url.
            links += [
                {**link, "href": self.resolve(link["href"])}
                for link in extra_links
                if link["rel"] not in INFERRED_LINK_RELS
            ]

        return links


@attr.s
class PagingLinks(BaseLinks):
    """Create links for paging."""

    next: Optional[str] = attr.ib(kw_only=True, default=None)
    prev: Optional[str] = attr.ib(kw_only=True, default=None)

    def link_next(self) -> Optional[Dict[str, Any]]:
        """Create link for next page."""
        if self.next is not None:
            method = self.request.method
            if method == "GET":
                href = merge_params(self.url, {"token": f"next:{self.next}"})
                link = {
                    "rel": Relations.next.value,
                    "type": MimeTypes.geojson.value,
                    "method": method,
                    "href": href,
                }
                return link

            if method == "POST":
                return {
                    "rel": Relations.next.value,
                    "type": MimeTypes.geojson.value,
                    "method": method,
                    "href": self.url,
                    "body": {**self.request.postbody, "token": f"next:{self.next}"},
                }

        return None

    def link_prev(self) -> Optional[Dict[str, Any]]:
        """Create link for previous page."""
        if self.prev is not None:
            method = self.request.method
            if method == "GET":
                href = merge_params(self.url, {"token": f"prev:{self.prev}"})
                return {
                    "rel": Relations.previous.value,
                    "type": MimeTypes.geojson.value,
                    "method": method,
                    "href": href,
                }

            if method == "POST":
                return {
                    "rel": Relations.previous.value,
                    "type": MimeTypes.geojson.value,
                    "method": method,
                    "href": self.url,
                    "body": {**self.request.postbody, "token": f"prev:{self.prev}"},
                }
        return None


@attr.s
class CollectionSearchPagingLinks(BaseLinks):
    next: Optional[Dict[str, Any]] = attr.ib(kw_only=True, default=None)
    prev: Optional[Dict[str, Any]] = attr.ib(kw_only=True, default=None)

    def link_next(self) -> Optional[Dict[str, Any]]:
        """Create link for next page."""
        if self.next is not None:
            method = self.request.method
            if method == "GET":
                # if offset is equal to default value (0), drop it
                if self.next["body"].get("offset", -1) == 0:
                    _ = self.next["body"].pop("offset")

                href = merge_params(self.url, self.next["body"])

                # if next link is equal to this link, skip it
                if href == self.url:
                    return None

                return {
                    "rel": Relations.next.value,
                    "type": MimeTypes.geojson.value,
                    "method": method,
                    "href": href,
                }

        return None

    def link_prev(self):
        if self.prev is not None:
            method = self.request.method
            if method == "GET":
                href = merge_params(self.url, self.prev["body"])

                # if prev link is equal to this link, skip it
                if href == self.url:
                    return None

                return {
                    "rel": Relations.previous.value,
                    "type": MimeTypes.geojson.value,
                    "method": method,
                    "href": href,
                }

        return None


@attr.s
class CollectionLinksBase(BaseLinks):
    """Create inferred links specific to collections."""

    collection_id: str = attr.ib()

    def collection_link(self, rel: str = Relations.collection.value) -> Dict:
        """Create a link to a collection."""
        return {
            "rel": rel,
            "type": MimeTypes.json.value,
            "href": self.resolve(f"collections/{self.collection_id}"),
        }


@attr.s
class CollectionLinks(CollectionLinksBase):
    """Create inferred links specific to collections."""

    def link_self(self) -> Dict:
        """Return the self link."""
        return self.collection_link(rel=Relations.self.value)

    def link_parent(self) -> Dict:
        """Create the `parent` link."""
        return {
            "rel": Relations.parent.value,
            "type": MimeTypes.json.value,
            "href": self.base_url,
        }

    def link_items(self) -> Dict:
        """Create the `item` link."""
        return {
            "rel": "items",
            "type": MimeTypes.geojson.value,
            "href": self.resolve(f"collections/{self.collection_id}/items"),
        }


@attr.s
class SearchLinks(BaseLinks):
    """Create inferred links specific to collections."""

    def link_self(self) -> Dict:
        """Return the self link."""
        return {
            "rel": Relations.self.value,
            "type": MimeTypes.geojson.value,
            "href": self.resolve("search"),
        }


@attr.s
class ItemCollectionLinks(CollectionLinksBase):
    """Create inferred links specific to collections."""

    def link_self(self) -> Dict:
        """Return the self link."""
        return {
            "rel": Relations.self.value,
            "type": MimeTypes.geojson.value,
            "href": self.resolve(f"collections/{self.collection_id}/items"),
        }

    def link_parent(self) -> Dict:
        """Create the `parent` link."""
        return self.collection_link(rel=Relations.parent.value)

    def link_collection(self) -> Dict:
        """Create the `collection` link."""
        return self.collection_link()


@attr.s
class ItemLinks(CollectionLinksBase):
    """Create inferred links specific to items."""

    item_id: str = attr.ib()

    def link_self(self) -> Dict:
        """Create the self link."""
        return {
            "rel": Relations.self.value,
            "type": MimeTypes.geojson.value,
            "href": self.resolve(
                f"collections/{self.collection_id}/items/{self.item_id}"
            ),
        }

    def link_parent(self) -> Dict:
        """Create the `parent` link."""
        return self.collection_link(rel=Relations.parent.value)

    def link_collection(self) -> Dict:
        """Create the `collection` link."""
        return self.collection_link()

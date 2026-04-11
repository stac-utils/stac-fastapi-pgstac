"""Link helpers for catalogs."""

import attr
from stac_pydantic.links import Relations
from stac_pydantic.shared import MimeTypes

from stac_fastapi.pgstac.models.links import BaseLinks


@attr.s
class CatalogLinks(BaseLinks):
    """Create inferred links specific to catalogs."""

    catalog_id: str = attr.ib()
    parent_ids: list[str] = attr.ib(kw_only=True, factory=list)
    child_catalog_ids: list[str] = attr.ib(kw_only=True, factory=list)

    def link_self(self) -> dict:
        """Return the self link."""
        return {
            "rel": Relations.self.value,
            "type": MimeTypes.json.value,
            "href": self.resolve(f"catalogs/{self.catalog_id}"),
        }

    def link_parent(self) -> dict | None:
        """Create the `parent` link."""
        if self.parent_ids:
            # Nested catalog: parent link to first parent
            return {
                "rel": Relations.parent.value,
                "type": MimeTypes.json.value,
                "href": self.resolve(f"catalogs/{self.parent_ids[0]}"),
                "title": self.parent_ids[0],
            }
        else:
            # Top-level catalog: parent link to root
            return {
                "rel": Relations.parent.value,
                "type": MimeTypes.json.value,
                "href": self.base_url,
                "title": "Root Catalog",
            }

    def link_child(self) -> list[dict] | None:
        """Create `child` links for sub-catalogs found in database."""
        if not self.child_catalog_ids:
            return None

        # Return list of child links - one for each child catalog
        return [
            {
                "rel": "child",
                "type": MimeTypes.json.value,
                "href": self.resolve(f"catalogs/{child_id}"),
                "title": child_id,
            }
            for child_id in self.child_catalog_ids
        ]


@attr.s
class CatalogSubcatalogsLinks(BaseLinks):
    """Create inferred links for sub-catalogs listing."""

    catalog_id: str = attr.ib()
    next_token: str | None = attr.ib(kw_only=True, default=None)
    limit: int = attr.ib(kw_only=True, default=10)

    def link_self(self) -> dict:
        """Return the self link."""
        return {
            "rel": Relations.self.value,
            "type": MimeTypes.json.value,
            "href": self.resolve(f"catalogs/{self.catalog_id}/catalogs"),
            "title": "Sub-catalogs",
        }

    def link_parent(self) -> dict:
        """Create the `parent` link."""
        return {
            "rel": Relations.parent.value,
            "type": MimeTypes.json.value,
            "href": self.resolve(f"catalogs/{self.catalog_id}"),
            "title": "Parent Catalog",
        }

    def link_next(self) -> dict | None:
        """Create link for next page."""
        if self.next_token is not None:
            return {
                "rel": Relations.next.value,
                "type": MimeTypes.json.value,
                "href": self.resolve(
                    f"catalogs/{self.catalog_id}/catalogs?limit={self.limit}&token={self.next_token}"
                ),
            }
        return None

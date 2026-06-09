"""Link helpers for catalogs."""

import attr
from stac_pydantic.links import Relations
from stac_pydantic.shared import MimeTypes

from stac_fastapi.pgstac.models.links import BaseLinks


@attr.s
class CatalogLinks(BaseLinks):
    """Create inferred links specific to catalogs.

    Generates self, parent, and child links for a catalog based on its
    position in the hierarchy and child catalogs.

    Attributes:
        catalog_id: The ID of the catalog.
        parent_ids: List of parent catalog IDs (empty for root).
        child_catalog_ids: List of child catalog IDs.
    """

    catalog_id: str = attr.ib()
    parent_ids: list[str] = attr.ib(kw_only=True, factory=list)
    child_catalog_ids: list[str] = attr.ib(kw_only=True, factory=list)

    def link_self(self) -> dict:
        """Return the self link.

        Returns:
            A link dict with rel='self' pointing to this catalog.
        """
        return {
            "rel": Relations.self.value,
            "type": MimeTypes.json.value,
            "href": self.resolve(f"catalogs/{self.catalog_id}"),
        }

    def link_parent(self) -> dict | None:
        """Create the `parent` link.

        For nested catalogs, points to the first parent catalog.
        For root catalogs, points to the root catalog.

        Returns:
            A link dict with rel='parent', or None if no parent.
        """
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
        """Create `child` links for sub-catalogs found in database.

        Returns:
            A list of link dicts with rel='child' for each child catalog,
            or None if no children.
        """
        if not self.child_catalog_ids:
            return None

        # Return list of child links - one for each child catalog
        return [
            {
                "rel": "child",
                "type": MimeTypes.json.value,
                "href": self.resolve(f"catalogs/{child_id}"),
            }
            for child_id in self.child_catalog_ids
        ]

    def link_root(self) -> dict:
        """Return the root catalog link.

        Returns:
            A link dict with rel='root' pointing to the global root.
        """
        return {
            "rel": Relations.root.value,
            "type": MimeTypes.json.value,
            "href": self.base_url,
        }

    def link_data(self) -> dict:
        """Return the data link to collections endpoint.

        Returns:
            A link dict with rel='data' pointing to the collections endpoint.
        """
        return {
            "rel": "data",
            "type": MimeTypes.json.value,
            "href": self.resolve(f"catalogs/{self.catalog_id}/collections"),
        }

    def link_catalogs(self) -> dict:
        """Return the catalogs link to sub-catalogs endpoint.

        Returns:
            A link dict pointing to the sub-catalogs endpoint.
        """
        return {
            "rel": "catalogs",
            "type": MimeTypes.json.value,
            "href": self.resolve(f"catalogs/{self.catalog_id}/catalogs"),
        }

    def link_children(self) -> dict:
        """Return the children link to children endpoint.

        Returns:
            A link dict pointing to the children endpoint.
        """
        return {
            "rel": "children",
            "type": MimeTypes.json.value,
            "href": self.resolve(f"catalogs/{self.catalog_id}/children"),
        }


@attr.s
class ChildLinks(BaseLinks):
    """Create inferred links for a child (catalog or collection) in a scoped context.

    Generates self, parent, and root links for a child accessed through
    a parent catalog's children endpoint.

    Attributes:
        catalog_id: The ID of the parent catalog.
        child_id: The ID of the child (catalog or collection).
        child_type: The type of the child ('Catalog' or 'Collection').
        parent_ids: List of parent catalog IDs (for poly-hierarchy).
    """

    catalog_id: str = attr.ib()
    child_id: str = attr.ib()
    child_type: str = attr.ib()
    parent_ids: list[str] = attr.ib(kw_only=True, factory=list)

    def link_self(self) -> dict:
        """Return the self link pointing to this child in scoped context.

        Returns:
            A link dict with rel='self' pointing to the scoped child.
        """
        if self.child_type == "Catalog":
            href = self.resolve(f"catalogs/{self.catalog_id}/catalogs/{self.child_id}")
        else:  # Collection
            href = self.resolve(f"catalogs/{self.catalog_id}/collections/{self.child_id}")

        return {
            "rel": Relations.self.value,
            "type": MimeTypes.json.value,
            "href": href,
        }

    def link_parent(self) -> dict:
        """Create the `parent` link pointing to the parent catalog.

        Returns:
            A link dict with rel='parent' pointing to the parent catalog.
        """
        return {
            "rel": Relations.parent.value,
            "type": MimeTypes.json.value,
            "href": self.resolve(f"catalogs/{self.catalog_id}"),
        }

    def link_root(self) -> dict:
        """Return the root catalog link.

        Returns:
            A link dict with rel='root' pointing to the global root.
        """
        return {
            "rel": Relations.root.value,
            "type": MimeTypes.json.value,
            "href": self.base_url,
        }

    def link_related(self) -> list[dict] | None:
        """Create related links for alternative parents (poly-hierarchy).

        Returns:
            A list of link dicts with rel='related' for other parents, or None.
        """
        if not self.parent_ids or len(self.parent_ids) <= 1:
            return None

        related_links = []
        for parent_id in self.parent_ids:
            if parent_id != self.catalog_id:  # Don't link to self
                if self.child_type == "Catalog":
                    href = self.resolve(f"catalogs/{parent_id}/catalogs/{self.child_id}")
                else:  # Collection
                    href = self.resolve(
                        f"catalogs/{parent_id}/collections/{self.child_id}"
                    )

                related_links.append(
                    {
                        "rel": "related",
                        "type": MimeTypes.json.value,
                        "href": href,
                    }
                )
        return related_links if related_links else None


@attr.s
class SubCatalogLinks(BaseLinks):
    """Create inferred links for a single sub-catalog in a scoped context.

    Generates self, parent, and root links for a sub-catalog accessed through
    a parent catalog endpoint.

    Attributes:
        catalog_id: The ID of the parent catalog.
        sub_catalog_id: The ID of the sub-catalog.
        parent_ids: List of parent catalog IDs (for poly-hierarchy).
    """

    catalog_id: str = attr.ib()
    sub_catalog_id: str = attr.ib()
    parent_ids: list[str] = attr.ib(kw_only=True, factory=list)

    def link_self(self) -> dict:
        """Return the self link pointing to this sub-catalog in scoped context.

        Returns:
            A link dict with rel='self' pointing to the scoped sub-catalog.
        """
        return {
            "rel": Relations.self.value,
            "type": MimeTypes.json.value,
            "href": self.resolve(
                f"catalogs/{self.catalog_id}/catalogs/{self.sub_catalog_id}"
            ),
        }

    def link_parent(self) -> dict:
        """Create the `parent` link pointing to the parent catalog.

        Returns:
            A link dict with rel='parent' pointing to the parent catalog.
        """
        return {
            "rel": Relations.parent.value,
            "type": MimeTypes.json.value,
            "href": self.resolve(f"catalogs/{self.catalog_id}"),
        }

    def link_root(self) -> dict:
        """Return the root catalog link.

        Returns:
            A link dict with rel='root' pointing to the global root.
        """
        return {
            "rel": Relations.root.value,
            "type": MimeTypes.json.value,
            "href": self.base_url,
        }

    def link_related(self) -> list[dict] | None:
        """Create related links for alternative parents (poly-hierarchy).

        Returns:
            A list of link dicts with rel='related' for other parents, or None.
        """
        if not self.parent_ids or len(self.parent_ids) <= 1:
            return None

        related_links = []
        for parent_id in self.parent_ids:
            if parent_id != self.catalog_id:  # Don't link to self
                related_links.append(
                    {
                        "rel": "related",
                        "type": MimeTypes.json.value,
                        "href": self.resolve(
                            f"catalogs/{parent_id}/catalogs/{self.sub_catalog_id}"
                        ),
                    }
                )
        return related_links if related_links else None


@attr.s
class CatalogSubcatalogsLinks(BaseLinks):
    """Create inferred links for sub-catalogs listing.

    Generates self, parent, and next links for a paginated list of sub-catalogs.

    Attributes:
        catalog_id: The ID of the parent catalog.
        next_token: Pagination token for the next page (if any).
        limit: The number of results per page.
    """

    catalog_id: str = attr.ib()
    next_token: str | None = attr.ib(kw_only=True, default=None)
    limit: int = attr.ib(kw_only=True, default=10)

    def link_self(self) -> dict:
        """Return the self link.

        Returns:
            A link dict with rel='self' pointing to the sub-catalogs listing.
        """
        return {
            "rel": Relations.self.value,
            "type": MimeTypes.json.value,
            "href": self.resolve(f"catalogs/{self.catalog_id}/catalogs"),
            "title": "Sub-catalogs",
        }

    def link_parent(self) -> dict:
        """Create the `parent` link.

        Returns:
            A link dict with rel='parent' pointing to the parent catalog.
        """
        return {
            "rel": Relations.parent.value,
            "type": MimeTypes.json.value,
            "href": self.resolve(f"catalogs/{self.catalog_id}"),
            "title": "Parent Catalog",
        }

    def link_next(self) -> dict | None:
        """Create link for next page.

        Returns:
            A link dict with rel='next' for pagination, or None if no next page.
        """
        if self.next_token is not None:
            return {
                "rel": Relations.next.value,
                "type": MimeTypes.json.value,
                "href": self.resolve(
                    f"catalogs/{self.catalog_id}/catalogs?limit={self.limit}&token={self.next_token}"
                ),
            }
        return None

"""stac-fastapi utility methods."""

from typing import Any, cast

from stac_fastapi.types.stac import Item


def clean_exclude(
    include: set[str],
    exclude: set[str],
) -> set[str]:
    """Clean the exclude set to ensure precedence of the include set.

    Cleaning includes:
    - Removing any fields from the exclude set that are also in the include set, since
      the include set takes precedence.
    - Removing any fields from the exclude set that are parent paths of fields in the include set,
      since including a sub-field of an excluded parent field should take precedence.
    """
    intersection = include.intersection(exclude)
    if intersection:
        exclude = exclude - intersection
    for field_excluded in exclude:
        for field_included in include:
            if field_included.startswith(field_excluded + "."):
                exclude = exclude - {field_excluded}
                pass
    return exclude


def dict_deep_update(merge_to: dict[str, Any], merge_from: dict[str, Any]) -> None:
    """Perform a deep update of two dicts.

    merge_to is updated in-place with the values from merge_from.
    merge_from values take precedence over existing values in merge_to.
    """
    for k, v in merge_from.items():
        if (
            k in merge_to
            and isinstance(merge_to[k], dict)
            and isinstance(merge_from[k], dict)
        ):
            dict_deep_update(merge_to[k], merge_from[k])
        else:
            merge_to[k] = v


def include_fields(source: dict[str, Any], fields: set[str]) -> dict[str, Any]:
    # Build a shallow copy of included fields on an item, or a sub-tree of an item
    if not fields:
        return source

    clean_item: dict[str, Any] = {}
    for key_path in fields:
        key_path_parts = key_path.split(".")
        key_root = key_path_parts[0]
        if key_root in source:
            if isinstance(source[key_root], dict) and len(key_path_parts) > 1:
                # The root of this key path on the item is a dict, and the
                # key path indicates a sub-key to be included. Walk the dict
                # from the root key and get the full nested value to include.
                value = include_fields(
                    source[key_root], fields={".".join(key_path_parts[1:])}
                )

                if isinstance(clean_item.get(key_root), dict):
                    # A previously specified key and sub-keys may have been included
                    # already, so do a deep merge update if the root key already exists.
                    dict_deep_update(clean_item[key_root], value)
                else:
                    # The root key does not exist, so add it. Fields
                    # extension only allows nested referencing on dicts, so
                    # this won't overwrite anything.
                    clean_item[key_root] = value
            else:
                # The item value to include is not a dict, or, it is a dict but the
                # key path is for the whole value, not a sub-key. Include the entire
                # value in the cleaned item.
                clean_item[key_root] = source[key_root]
        else:
            # The key, or root key of a multi-part key, is not present in the item,
            # so it is ignored
            pass

    return clean_item


def exclude_fields(source: dict[str, Any], fields: set[str]) -> None:
    # For an item built up for included fields, remove excluded fields. This
    # modifies `source` in place.
    for key_path in fields:
        key_path_part = key_path.split(".")
        key_root = key_path_part[0]
        if key_root in source:
            if isinstance(source[key_root], dict) and len(key_path_part) > 1:
                # Walk the nested path of this key to remove the leaf-key
                exclude_fields(source[key_root], fields={".".join(key_path_part[1:])})
                # If, after removing the leaf-key, the root is now an empty
                # dict, remove it entirely
                if not source[key_root]:
                    del source[key_root]
            else:
                # The key's value is not a dict, or there is no sub-key to remove. The
                # entire key can be removed from the source.
                source.pop(key_root, None)
        else:
            # The key to remove does not exist on the source, so it is ignored
            pass


def filter_fields(  # noqa: C901
    item: Item,
    include: set[str],
    exclude: set[str],
) -> Item:
    """Preserve and remove fields as indicated by the fields extension include/exclude sets.

    Returns a shallow copy of the Item with the fields filtered.

    This will not perform a deep copy; values of the original item will be referenced
    in the return item.
    """
    exclude = clean_exclude(include, exclude)

    if not include and not exclude:
        return item

    clean_item = include_fields(dict(item), include)

    # If, after including all the specified fields, there are no included properties,
    # return just id and collection.
    if not clean_item:
        return Item({"id": item["id"], "collection": item["collection"]})  # type: ignore

    exclude_fields(clean_item, exclude)

    return cast(Item, clean_item)

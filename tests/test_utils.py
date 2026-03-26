import pytest

import stac_fastapi.pgstac.utils as utils


def test_dict_deep_update():
    dict_0 = {"a": 0, "b": 0, "c": {"c1": 0, "c2": 0}, "d": {"d1": 0}, "e": 0, "f": 0}
    dict_1 = {"b": 1, "c": {"c2": 1, "c3": 1}, "d": 1, "e": {"e1": 1}, "f": 1}
    utils.dict_deep_update(dict_0, dict_1)
    exp = {
        "a": 0,
        "b": 1,
        "c": {"c1": 0, "c2": 1, "c3": 1},
        "d": 1,
        "e": {"e1": 1},
        "f": 1,
    }
    assert dict_0 == exp


def test_include_fields_no_fields():
    source = {"a": 0}
    res = utils.include_fields(source, fields=None)
    assert res == source


def test_include_fields():
    source = {"a": 0, "b": 0, "c": {"c1": 0, "c2": 0, "c3": 0}, "d": {"d1": 0}, "e": 0}
    fields = {"a", "c.c1", "c.c2", "d", "f"}
    res = utils.include_fields(source, fields=fields)
    exp = {"a": 0, "c": {"c1": 0, "c2": 0}, "d": {"d1": 0}}
    assert res == exp


def test_exclude_fields():
    source = {
        "a": 0,
        "b": 0,
        "c": {"c1": 0, "c2": 0},
        "d": {"d1": 0},
        "e": {"e1": 0},
        "f": 0,
    }
    fields = {"a", "c.c1", "d.d1", "e"}
    utils.exclude_fields(source, fields=fields)
    exp = {"b": 0, "c": {"c2": 0}, "f": 0}
    assert source == exp


def test_filter_fields_no_included_properties():
    item = utils.Item(
        id="test_id",
        collection="test_collection",
        properties={"prop_1": 0, "prop_2": 0},
    )
    res = utils.filter_fields(item, include={"missing_field"})
    exp = utils.Item(id="test_id", collection="test_collection")
    assert res == exp


@pytest.mark.parametrize(
    "include, exclude, exp",
    [
        ({"properties"}, {"properties.prop_1"}, {"properties": {"prop_2": 0}}),
    ],
    ids=["include root, exclude nested"],
)
def test_filter_fields(include, exclude, exp):
    source = utils.Item(
        id="test_id",
        collection="test_collection",
        properties={"prop_1": 0, "prop_2": 0},
    )
    res = utils.filter_fields(source, include=include, exclude=exclude)
    assert res == utils.Item(**exp)

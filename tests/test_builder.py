import os
import pytest
from bigquery_builder import Builder

@pytest.fixture(scope='module')
def builder():
    builder = Builder(os.path.join(".", "models"))
    yield builder

def test_parser(builder):
    parsed_data = builder.view_parser("test1")
    assert len(parsed_data["nodes"]) == 2
    for node in parsed_data["nodes"]:
        print(node["dependencies"])

def test_view_create(builder):
    view_id = builder.create_view("test_builder", "test1")
    url1 = builder.get_preview_url("test_builder", "test1")
    assert view_id.endswith("test1")
    assert "https" in url1
    view_id = builder.create_view("test_builder", "test1", "Base")
    url2 = builder.get_preview_url("test_builder", "test1", "Base")
    assert not view_id.endswith("test1")
    assert url1 != url2

def test_table_create(builder):
    table_id = builder.create_table("test_builder", "test2")
    assert table_id.endswith("test2")

def test_exceptions():
    with pytest.raises(ValueError):
        b = Builder("dummy")

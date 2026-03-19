import pytest
from dwh2looker.lookml_generator.generators import NestedFieldHelper
from jinja2 import Environment, FileSystemLoader


@pytest.fixture
def jinja_env():
    return Environment(
        loader=FileSystemLoader("src/dwh2looker/lookml_generator/templates/")
    )


@pytest.fixture
def nested_field_helper():
    return NestedFieldHelper()

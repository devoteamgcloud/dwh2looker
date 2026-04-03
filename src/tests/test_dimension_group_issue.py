from unittest.mock import Mock
import pytest
from dwh2looker.db_client.db_client import Field
from dwh2looker.lookml_generator.generators import (
    DimensionGroupGenerator,
    NestedFieldHelper,
)


@pytest.fixture
def jinja_env():
    return Mock()


@pytest.fixture
def nested_field_helper():
    return NestedFieldHelper()


def test_create_dimension_group_in_nullable_record(jinja_env, nested_field_helper):
    """
    REPRODUCTION TEST CASE:
    When a field is inside a nullable record (e.g., 'product.created'),
    the generated SQL should be '${TABLE}.product.created'.
    """
    dimension_group_generator = DimensionGroupGenerator(
        timeframes=["time", "date", "week", "month"],
        time_suffixes=["_ts"],
        primary_key_prefixes=["pk_"],
        jinja_env=jinja_env,
        nested_field_helper=nested_field_helper,
    )

    # Simulate a field 'created' inside a 'product' record
    field = Mock(spec=Field)
    field.name = "created_ts"
    field.internal_type = "TIMESTAMP"
    field.mode = "NULLABLE"
    field.description = "Product creation timestamp"
    field.parent_field_name = "product"
    field.parent_field_type = "RECORD_NULLABLE"

    dimension_group = dimension_group_generator.create_dimension_group(field)

    # EXPECTED: ${TABLE}.product.created_ts
    # ACTUAL (Current Bug): ${TABLE}.created_ts
    assert dimension_group.sql == "${TABLE}.product.created_ts", (
        f"Expected ${{TABLE}}.product.created_ts but got {dimension_group.sql}"
    )

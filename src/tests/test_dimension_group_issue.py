import pytest
from unittest.mock import Mock
from dwh2looker.db_client.db_client import Field
from dwh2looker.lookml_generator.generators import DimensionGroupGenerator, NestedFieldHelper

def test_create_dimension_group_in_nullable_record():
    """
    REPRODUCTION TEST CASE:
    When a field is inside a nullable record (e.g., 'product.created'),
    the generated SQL should be '${TABLE}.product.created'.
    """
    nested_field_helper = NestedFieldHelper()
    jinja_env = Mock()
    
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
    assert dimension_group.sql == "${TABLE}.product.created_ts", f"Expected ${{TABLE}}.product.created_ts but got {dimension_group.sql}"

if __name__ == "__main__":
    try:
        test_create_dimension_group_in_nullable_record()
        print("Test passed!")
    except AssertionError as e:
        print(f"Test failed: {e}")

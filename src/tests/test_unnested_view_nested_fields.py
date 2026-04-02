import pytest
from unittest.mock import Mock, patch
from dwh2looker.lookml_generator.lookml_generator import LookMLGenerator
from dwh2looker.db_client.db_client import Field, Table

def test_nested_fields_in_unnested_view():
    """
    REPRODUCTION TEST CASE for deeply nested fields in unnested views.
    In an unnested view (e.g., order_items), the SQL for a nested field 
    (e.g., item_meta.id) should be relative to ${TABLE} (the array element).
    Current BUG: It includes the parent array name (e.g., ${TABLE}.order_items.item_meta.id).
    """
    # 1. Define fields
    # order_items (ARRAY/RECORD_REPEATED)
    order_items_field = Field(
        name="order_items",
        internal_type="RECORD",
        mode="REPEATED",
        description="List of items"
    )
    # item_meta (STRUCT/RECORD_NULLABLE inside order_items)
    item_meta_field = Field(
        name="item_meta",
        internal_type="RECORD",
        mode="NULLABLE",
        description="Item metadata",
        parent_field_name="order_items",
        parent_field_type="RECORD_REPEATED"
    )
    # meta_trigger_id (STRING inside item_meta)
    meta_trigger_id_field = Field(
        name="meta_trigger_id",
        internal_type="STRING",
        mode="NULLABLE",
        description="Trigger ID",
        parent_field_name="order_items.item_meta",
        parent_field_type="RECORD_NULLABLE"
    )

    # 2. Mock Table and Schema
    mock_table = Table(name="fact__interactions", internal_schema=[])
    # The schema structure used by LookMLGenerator:
    # { field_obj: { sub_field_obj: {} } }
    mock_table.schema = {
        order_items_field: {
            item_meta_field: {
                meta_trigger_id_field: {}
            }
        }
    }

    # 3. Setup LookMLGenerator
    with patch("dwh2looker.lookml_generator.lookml_generator.Config") as MockConfig:
        mock_config = Mock()
        MockConfig.return_value = mock_config
        mock_config.get_property.return_value = [] # Default for most properties
        # Special case for timeframes and prefixes
        def get_prop(prop, default=None):
            if prop == "primary_key_prefixes": return ["pk_"]
            if prop == "timeframes": return ["time", "date"]
            return default
        mock_config.get_property.side_effect = get_prop

        generator = LookMLGenerator(db_type="bigquery")
        
        # 4. Process views
        # We expect two views: fact__interactions and fact__interactions__order_items
        views = list(generator.process_views(
            schema=mock_table.schema,
            view_name=mock_table.name,
            sql_table_name="project.dataset.fact__interactions"
        ))

        # 5. Verify the order_items view
        # views is a list of tuples: (rendered_string, view_object)
        order_items_view_tuple = next((v for v in views if v[1].name == "order_items"), None)
        assert order_items_view_tuple is not None, "order_items view not generated"
        
        rendered_content, view_obj = order_items_view_tuple
        print(f"Generated View Name: {view_obj.name}")
        print("Rendered content:")
        print(rendered_content)

        # FIND meta_trigger_id dimension
        # EXPECTED SQL: ${TABLE}.item_meta.meta_trigger_id
        # ACTUAL (BUG): ${TABLE}.order_items.item_meta.meta_trigger_id
        assert "sql: ${TABLE}.item_meta.meta_trigger_id" in rendered_content, \
            f"Incorrect SQL for meta_trigger_id in unnested view. Content:\n{rendered_content}"

if __name__ == "__main__":
    try:
        test_nested_fields_in_unnested_view()
        print("Test PASSED!")
    except AssertionError as e:
        print(f"Test FAILED: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

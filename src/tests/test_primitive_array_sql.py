from unittest.mock import Mock, patch
from dwh2looker.lookml_generator.lookml_generator import LookMLGenerator
from dwh2looker.db_client.db_client import Field, Table


def test_primitive_array_unnested_view_sql():
    """
    Test that a primitive array unnested view has 'sql: ${TABLE}' for its elements,
    not '${TABLE}.array_name'.
    """
    # 1. Define fields for dim_demo_showcase__audit_log__features_used
    # We simulate a nested structure: audit_log (RECORD REPEATED) -> features_used (STRING REPEATED)

    audit_log_field = Field(
        name="audit_log",
        internal_type="RECORD",
        mode="REPEATED",
        description="Audit log array",
    )

    features_used_field = Field(
        name="features_used",
        internal_type="STRING",
        mode="REPEATED",
        description="List of features",
        parent_field_name="audit_log",
        parent_field_type="RECORD_REPEATED",
    )

    # Primitive array elements
    features_used_element_field = Field(
        name="features_used",
        internal_type="STRING",
        mode="NULLABLE",
        description="Elements of the array field: features_used",
        parent_field_name="audit_log.features_used",
        parent_field_type="ARRAY",
    )

    mock_table = Table(name="dim_demo_showcase", internal_schema=[])

    # Simulating the deeply nested schema
    mock_table.schema = {
        audit_log_field: {features_used_field: {features_used_element_field: {}}}
    }

    # 3. Setup LookMLGenerator
    with patch("dwh2looker.lookml_generator.lookml_generator.Config") as MockConfig:
        mock_config = Mock()
        MockConfig.return_value = mock_config
        mock_config.get_property.return_value = []

        def get_prop(prop, default=None):
            if prop == "primary_key_prefixes":
                return ["pk_"]
            if prop == "timeframes":
                return ["time", "date"]
            return default

        mock_config.get_property.side_effect = get_prop
        generator = LookMLGenerator(db_type="bigquery")

        # 4. Process views
        views = list(
            generator.process_views(
                schema=mock_table.schema,
                view_name=mock_table.name,
                sql_table_name="project.dataset.dim_demo_showcase",
            )
        )

        # 5. Verify the features_used view
        features_used_view_tuple = next(
            (v for v in views if v[1].name == "features_used"), None
        )
        assert features_used_view_tuple is not None, "features_used view not generated"

        rendered_content, _ = features_used_view_tuple
        print(f"\nRendered content for features_used view:\n{rendered_content}")

        # The expected SQL for a primitive array element when unnested is ${TABLE}
        assert (
            "sql: ${TABLE} ;;" in rendered_content
        ), f"Expected 'sql: ${{TABLE}} ;;' but got:\n{rendered_content}"
        assert "sql: ${TABLE}.features_used ;;" not in rendered_content

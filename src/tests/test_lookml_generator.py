from unittest.mock import Mock, patch

import pytest
from dwh2looker.db_client.db_client import Field
from dwh2looker.lookml_generator.lookml_generator import LookMLGenerator
from dwh2looker.lookml_generator.models import View


@pytest.fixture
def mock_config():
    with patch("dwh2looker.lookml_generator.lookml_generator.Config") as MockConfig:
        mock_config_instance = Mock()
        MockConfig.return_value = mock_config_instance
        mock_config_instance.get_property.side_effect = [
            ["pk_"],
            [],
            [],
            ["time"],
            [],
            False,
            [],
            {},
            {},
        ]
        yield mock_config_instance


def test_lookml_generator_init(mock_config):
    generator = LookMLGenerator(db_type="bigquery")
    assert generator.db_type == "bigquery"
    assert mock_config.get_property.call_count == 9


def test_sort_fields(mock_config):
    generator = LookMLGenerator(db_type="bigquery")
    field1 = Field("pk_one", "STRING", "NULLABLE", "")
    field2 = Field("fk_one", "STRING", "NULLABLE", "")
    field3 = Field("bk_one", "STRING", "NULLABLE", "")
    field4 = Field("dimension_one", "STRING", "NULLABLE", "")
    field5 = Field("a_dimension", "STRING", "NULLABLE", "")

    fields = [field2, field1, field4, field3, field5]

    sorted_fields = generator.sort_fields(fields)

    assert [f.name for f in sorted_fields] == [
        "pk_one",
        "bk_one",
        "fk_one",
        "a_dimension",
        "dimension_one",
    ]


def test_process_field(mock_config):
    generator = LookMLGenerator(db_type="bigquery")
    # Test a simple dimension
    field = Field("test_dim", "STRING", "NULLABLE", "A test dimension")
    processed = list(generator.process_field(field))
    assert "dimension: test_dim" in processed[0]

    # Test a dimension group
    field = Field("test_ts", "TIMESTAMP", "NULLABLE", "A test timestamp")
    processed = list(generator.process_field(field))
    assert "dimension_group: test" in processed[0]

    # Test ignored field
    generator.ignore_column_types = ["GEOGRAPHY"]
    field = Field("test_geo", "GEOGRAPHY", "NULLABLE", "A test geography")
    processed = list(generator.process_field(field))
    assert not processed


def test_process_views(mock_config):
    generator = LookMLGenerator(db_type="bigquery")

    field1 = Field("column1", "STRING", "NULLABLE", "")
    field2 = Field("column2", "INTEGER", "NULLABLE", "")
    schema = {"test_table": {field1: {}, field2: {}}}

    processed_views = list(
        generator.process_views(schema, "test_table", "sql_table_name")
    )

    assert len(processed_views) == 1
    view_string, view_object = processed_views[0]

    assert "view: test_table" in view_string
    assert "sql_table_name: `sql_table_name`" in view_string
    assert "dimension: column1" in view_string
    assert "dimension: column2" in view_string
    assert isinstance(view_object, View)


def test_process_joins(mock_config):
    generator = LookMLGenerator(db_type="bigquery")

    View(
        name="base_view",
        sql_table_name="sql_base",
        fields=[],
        full_view_path="base_view",
    )
    view2 = View(
        name="nested_view",
        sql_table_name=None,
        fields=[],
        full_view_path="base_view.nested_view",
    )

    joins = generator.process_joins("Base View", [view2])

    assert len(joins) == 1
    assert "join: base_view__nested_view" in joins[0]
    assert 'view_label: "Base View"' in joins[0]
    assert "relationship: one_to_many" in joins[0]


def test_process_explore(mock_config):
    generator = LookMLGenerator(db_type="bigquery")

    view1 = View(
        name="base_view",
        sql_table_name="sql_base",
        fields=[],
        full_view_path="base_view",
    )
    view2 = View(
        name="nested_view",
        sql_table_name=None,
        fields=[],
        full_view_path="base_view.nested_view",
    )
    views = [view1, view2]

    explore = generator.process_explore(views)

    assert "explore: base_view" in explore
    assert "join: base_view__nested_view" in explore

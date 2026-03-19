from unittest.mock import Mock, mock_open, patch

import pytest

from dwh2looker.db_client.db_client import Field
from dwh2looker.lookml_generator.generators import (
    DimensionGenerator,
    DimensionGroupGenerator,
    NestedFieldHelper,
    ViewGenerator,
)
from dwh2looker.lookml_generator.models import View


@pytest.fixture
def jinja_env():
    return Mock()


@pytest.fixture
def nested_field_helper():
    helper = NestedFieldHelper(capitalize_ids=False)
    return helper


def test_build_field_name():
    helper = NestedFieldHelper()
    assert helper.build_field_name("test_name") == "Test Name"
    assert helper.build_field_name("test_id") == "Test Id"


def test_build_field_name_capitalize_id():
    helper = NestedFieldHelper(capitalize_ids=True)
    assert helper.build_field_name("test_name") == "Test Name"
    assert helper.build_field_name("test_id") == "Test ID"


def test_create_dimension(jinja_env, nested_field_helper):
    dimension_generator = DimensionGenerator(
        db_type="bigquery",
        capitalize_ids=False,
        primary_key_prefixes=["pk_"],
        jinja_env=jinja_env,
        nested_field_helper=nested_field_helper,
    )

    # Test with a simple string field
    field = Mock()
    field.name = "test_field"
    field.internal_type = "STRING"
    field.description = "A test field"
    field.parent_field_name = None
    field.is_nested_field = False
    field.is_array_field = False
    field.mode = "NULLABLE"
    dimension = dimension_generator.create_dimension(field)
    assert dimension.name == "test_field"
    assert dimension.type == "string"
    assert dimension.sql == "${TABLE}.test_field"
    assert dimension.description == "A test field"
    assert dimension.is_nested is False
    assert dimension.group_label is None
    assert dimension.group_item_label is None

    # Test with a primary key
    field.name = "pk_test_id"
    field.parent_field_name = None
    field.is_nested_field = False
    field.is_array_field = False
    dimension = dimension_generator.create_dimension(field)
    assert dimension.pk == "yes"
    assert dimension.hidden == "yes"

    # Test with a foreign key
    field.name = "fk_test_id"
    field.parent_field_name = None
    field.is_nested_field = False
    field.is_array_field = False
    dimension = dimension_generator.create_dimension(field)
    assert dimension.hidden == "yes"

    # Test with a date field
    field.name = "test_date"
    field.internal_type = "DATE"
    field.parent_field_name = None
    field.is_nested_field = False
    field.is_array_field = False
    dimension = dimension_generator.create_dimension(field)
    assert dimension.datatype == "date"

    # Test with a nested field
    field.name = "nested_field"
    field.internal_type = "STRING"
    field.parent_field_name = "parent"
    field.is_nested_field = False
    field.is_array_field = False
    field.parent_field_type = "RECORD_NULLABLE"
    dimension = dimension_generator.create_dimension(field)
    assert dimension.name == "parent__nested_field"
    assert dimension.sql == "${TABLE}.parent.nested_field"
    assert dimension.group_label == "Parent"
    assert dimension.group_item_label == "Parent  Nested Field"
    assert dimension.is_nested is False

    # Test with a deeply nested field
    field.name = "grandchild_field"
    field.internal_type = "STRING"
    field.parent_field_name = "parent.child"
    field.is_nested_field = False
    field.is_array_field = False
    field.parent_field_type = "RECORD_NULLABLE"
    dimension = dimension_generator.create_dimension(field)
    assert dimension.name == "parent__child__grandchild_field"
    assert dimension.sql == "${TABLE}.parent.child.grandchild_field"
    assert dimension.group_label == "Parent Child"
    assert dimension.group_item_label == "Parent  Child  Grandchild Field"
    assert dimension.is_nested is False


def test_create_dimension_group(jinja_env, nested_field_helper):
    dimension_group_generator = DimensionGroupGenerator(
        timeframes=["time", "date", "week", "month"],
        time_suffixes=["_ts"],
        primary_key_prefixes=["pk_"],
        jinja_env=jinja_env,
        nested_field_helper=nested_field_helper,
    )

    # Test with a timestamp field
    field = Mock()
    field.name = "test_ts"
    field.internal_type = "TIMESTAMP"
    field.description = "A test timestamp"
    field.parent_field_name = None
    field.is_nested_field = False
    field.is_array_field = False
    dimension_group = dimension_group_generator.create_dimension_group(field)
    assert dimension_group.name == "test"
    assert dimension_group.timeframes == ["time", "date", "week", "month"]
    assert dimension_group.datatype == "timestamp"
    assert dimension_group.convert_tz == "no"

    # Test with a date field
    field.name = "test_date"
    field.internal_type = "DATE"
    field.parent_field_name = None
    field.is_nested_field = False
    field.is_array_field = False
    dimension_group = dimension_group_generator.create_dimension_group(field)
    assert dimension_group.timeframes == ["date", "week", "month"]

    # Test with a nested field
    field.name = "nested_ts"
    field.internal_type = "TIMESTAMP"
    field.parent_field_name = "parent"
    field.is_nested_field = False
    field.is_array_field = False
    dimension_group = dimension_group_generator.create_dimension_group(field)
    assert dimension_group.name == "nested"
    assert dimension_group.sql == "${TABLE}.nested_ts"
    assert dimension_group.group_label == "Parent"
    assert dimension_group.group_item_label == "Nested Ts"

    # Test with a deeply nested field
    field.name = "grandchild_ts"
    field.internal_type = "TIMESTAMP"
    field.parent_field_name = "parent.child"
    field.is_nested_field = False
    field.is_array_field = False
    dimension_group = dimension_group_generator.create_dimension_group(field)
    assert dimension_group.name == "grandchild"
    assert dimension_group.sql == "${TABLE}.grandchild_ts"
    assert dimension_group.group_label == "Parent Child"
    assert dimension_group.group_item_label == "Grandchild Ts"


def test_create_view(jinja_env):
    view_generator = ViewGenerator(jinja_env=jinja_env)
    view = view_generator.create_view(
        view_name="test_view",
        sql_table_name="test_table",
        fields=["field1", "field2"],
    )
    assert view.name == "test_view"
    assert view.sql_table_name == "test_table"
    assert view.fields == ["field1", "field2"]


@pytest.fixture
def lookml_generator():
    with patch("dwh2looker.lookml_generator.lookml_generator.Config") as mock_config:
        mock_config.return_value.get_property.return_value = ["pk_"]
        from dwh2looker.lookml_generator.lookml_generator import LookMLGenerator

        generator = LookMLGenerator(db_type="bigquery")
        return generator


def test_sort_fields(lookml_generator):
    field1 = Mock(spec=Field)
    field1.name = "pk_field"
    field1.is_nested_field = False
    field1.is_array_field = False
    field2 = Mock(spec=Field)
    field2.name = "fk_field"
    field2.is_nested_field = False
    field2.is_array_field = False
    field3 = Mock(spec=Field)
    field3.name = "bk_field"
    field3.is_nested_field = False
    field3.is_array_field = False
    field4 = Mock(spec=Field)
    field4.name = "other_field"
    field4.is_nested_field = False
    field4.is_array_field = False
    field5 = Mock(spec=Field)
    field5.name = "nested_field"
    field5.is_nested_field = True
    field5.is_array_field = False
    fields = [field4, field2, field5, field1, field3]
    sorted_fields = lookml_generator.sort_fields(fields)
    assert [f.name for f in sorted_fields] == [
        "pk_field",
        "bk_field",
        "fk_field",
        "other_field",
        "nested_field",
    ]


def test_create_explore(jinja_env):
    from dwh2looker.lookml_generator.generators import ExploreGenerator

    explore_generator = ExploreGenerator(jinja_env=jinja_env)
    view1 = Mock()
    view1.name = "view1"
    view1.full_view_path = "view1"
    view2 = Mock()
    view2.name = "view2"
    view2.full_view_path = "view2"
    explore = explore_generator.create_explore(
        explore_name="test_explore",
        view_label="Test Explore",
        hidden="yes",
        extension="required",
        joins=["join1", "join2"],
    )
    assert explore.name == "test_explore"
    assert explore.view_label == "Test Explore"
    assert explore.hidden == "yes"
    assert explore.extension == "required"
    assert explore.joins == ["join1", "join2"]


def test_create_refined_view(jinja_env):
    from dwh2looker.lookml_generator.generators import RefinedViewGenerator

    refined_view_generator = RefinedViewGenerator(jinja_env=jinja_env)
    view1 = View(
        name="view1", sql_table_name="table1", fields=[], full_view_path="view1"
    )
    view2 = View(
        name="view2", sql_table_name="table2", fields=[], full_view_path="view2"
    )
    refined_view = refined_view_generator.create_refined_view(
        include="/path/to/views/*.view.lkml", views=[view1, view2]
    )
    assert refined_view.include == "/path/to/views/*.view.lkml"
    assert refined_view.views == [view1, view2]


@patch("os.path.exists", return_value=False)
@patch("builtins.open", new_callable=mock_open)
def test_write_lookml(mock_open, mock_exists):
    from dwh2looker.lookml_generator.writer import LookMLFileWriter

    writer = LookMLFileWriter()
    writer.write_lookml(
        content="test content",
        file_name="test_file",
        type="view",
        output_dir="output",
    )
    mock_open.assert_called_with("output/test_file.view.lkml", "w")
    mock_open().write.assert_called_with("test content")

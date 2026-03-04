from unittest.mock import Mock, patch

import pytest
from dwh2looker.db_client.db_client import BQClient, DbClient, Field, Table


@patch("dwh2looker.db_client.db_client.import_module")
def test_bq_client_authentication_service_account(mock_import_module):
    mock_bigquery = Mock()
    mock_import_module.return_value = mock_bigquery
    client = BQClient(project_id="test-project", service_account="test-account.json")
    mock_bigquery.Client.from_service_account_json.assert_called_with(
        "test-account.json", project="test-project"
    )


@patch("dwh2looker.db_client.db_client.import_module")
def test_bq_client_authentication_oauth(mock_import_module):
    mock_bigquery = Mock()
    mock_import_module.return_value = mock_bigquery
    client = BQClient(project_id="test-project")
    mock_bigquery.Client.assert_called_with(project="test-project")


def test_db_client_factory():
    with patch("dwh2looker.db_client.db_client.BQClient") as mock_bq_client:
        client = DbClient(
            db_type="bigquery", credentials={"project_id": "test-project"}
        )
        mock_bq_client.assert_called_with(
            project_id="test-project",
            service_account=None,
            credentials_json_env_var=None,
        )

    with pytest.raises(Exception):
        DbClient(db_type="unsupported", credentials={})


@patch("dwh2looker.db_client.db_client.BQClient")
def test_get_table(mock_bq_client):
    mock_bq_client.return_value.get_table.return_value = "test_table"
    client = DbClient(db_type="bigquery", credentials={"project_id": "test-project"})
    table = client.get_table("test_dataset", "test_table")
    assert table == "test_table"


@patch("dwh2looker.db_client.db_client.BQClient")
def test_list_tables(mock_bq_client):
    mock_bq_client.return_value.list_tables.return_value = [
        "test_table1",
        "test_table2",
    ]
    client = DbClient(db_type="bigquery", credentials={"project_id": "test-project"})
    tables = client.list_tables("test_dataset")
    assert tables == ["test_table1", "test_table2"]


@patch("dwh2looker.db_client.db_client.BQClient")
def test_get_table_names(mock_bq_client):
    mock_bq_client.return_value.get_table_names.return_value = [
        "test_table1",
        "test_table2",
    ]
    client = DbClient(db_type="bigquery", credentials={"project_id": "test-project"})
    table_names = client.get_table_names("test_dataset")
    assert table_names == ["test_table1", "test_table2"]


def test_field_class():
    field = Field(
        name="test_field",
        internal_type="STRING",
        mode="NULLABLE",
        description="A test field",
    )
    assert field.name == "test_field"
    assert field.get_internal_type() == "STRING"
    assert field.get_mode() == "NULLABLE"
    assert field.get_description() == "A test field"


def test_table_class():
    table = Table(name="test_table", internal_schema="test_schema")
    assert table.name == "test_table"
    assert table.get_internal_schema() == "test_schema"
    assert table.get_table_name() == "test_table"

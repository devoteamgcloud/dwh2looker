import os
from unittest.mock import Mock, patch

from dwh2looker.db_client.db_client import BQClient, Field, Table


@patch("dwh2looker.db_client.db_client.import_module")
def test_bq_client_init_with_service_account(mock_import_module):
    mock_bigquery = Mock()
    mock_import_module.return_value = mock_bigquery
    BQClient(project_id="test-project", service_account="path/to/key.json")
    mock_bigquery.Client.from_service_account_json.assert_called_once_with(
        "path/to/key.json", project="test-project"
    )


@patch("dwh2looker.db_client.db_client.import_module")
def test_bq_client_init_with_oauth(mock_import_module):
    mock_bigquery = Mock()
    mock_import_module.return_value = mock_bigquery
    BQClient(project_id="test-project")
    mock_bigquery.Client.assert_called_once_with(project="test-project")


@patch.dict(os.environ, {"GCP_CREDENTIALS": '{"project_id": "env-project"}'})
@patch("dwh2looker.db_client.db_client.import_module")
def test_bq_client_init_with_env_var(mock_import_module):
    mock_bigquery = Mock()
    mock_google_auth = Mock()
    mock_import_module.side_effect = [mock_bigquery, mock_google_auth]
    credentials = Mock()
    mock_google_auth.load_credentials_from_dict.return_value = (
        credentials,
        "env-project",
    )
    BQClient(project_id="test-project", credentials_json_env_var="GCP_CREDENTIALS")
    mock_google_auth.load_credentials_from_dict.assert_called_once_with(
        {"project_id": "env-project"}
    )
    mock_bigquery.Client.assert_called_once_with(
        project="test-project", credentials=credentials
    )


@patch("dwh2looker.db_client.db_client.import_module")
def test_get_table(mock_import_module):
    mock_bigquery = Mock()
    mock_import_module.return_value = mock_bigquery

    mock_bq_client = Mock()
    mock_bigquery.Client.return_value = mock_bq_client

    mock_schema_field = Mock()
    mock_schema_field.name = "test_field"
    mock_schema_field.field_type = "STRING"
    mock_schema_field.mode = "NULLABLE"
    mock_schema_field.description = "A test field"
    mock_schema_field.fields = []

    mock_bq_table = Mock()
    mock_bq_table.table_id = "test_table"
    mock_bq_table.schema = [mock_schema_field]

    mock_bq_client.get_table.return_value = mock_bq_table

    client = BQClient(project_id="test-project")
    table = client.get_table("test_dataset", "test_table")

    assert isinstance(table, Table)
    assert table.name == "test_table"
    assert len(table.schema["test_table"]) == 1

    field = list(table.schema["test_table"].keys())[0]
    assert isinstance(field, Field)
    assert field.name == "test_field"


@patch("dwh2looker.db_client.db_client.import_module")
def test_list_tables(mock_import_module):
    mock_bigquery = Mock()
    mock_import_module.return_value = mock_bigquery

    mock_bq_client = Mock()
    mock_bigquery.Client.return_value = mock_bq_client

    mock_table_item = Mock()
    mock_table_item.table_id = "test_table"

    mock_bq_client.list_tables.return_value = [mock_table_item]

    # Mocking get_table to avoid its complex logic in this unit test
    with patch.object(
        BQClient, "get_table", return_value=Table(name="test_table", internal_schema=[])
    ) as mock_get_table:
        client = BQClient(project_id="test-project")
        tables = client.list_tables("test_dataset")
        assert len(tables) == 1
        assert tables[0].name == "test_table"
        mock_get_table.assert_called_once_with("test_dataset", "test_table")


@patch("dwh2looker.db_client.db_client.import_module")
def test_get_table_names(mock_import_module):
    mock_bigquery = Mock()
    mock_import_module.return_value = mock_bigquery

    mock_bq_client = Mock()
    mock_bigquery.Client.return_value = mock_bq_client

    mock_table_item = Mock()
    mock_table_item.table_id = "test_table"

    mock_bq_client.list_tables.return_value = [mock_table_item]

    client = BQClient(project_id="test-project")
    table_names = client.get_table_names("test_dataset")

    assert table_names == ["test_table"]

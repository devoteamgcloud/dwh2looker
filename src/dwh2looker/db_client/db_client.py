import json
import os
from importlib import import_module
from typing import Optional, Union

from dwh2looker.logger import Logger

CONSOLE_LOGGER = Logger().get_logger()


class DbClient:
    SUPPORTED_DATABASES = ["bigquery"]

    def __init__(self, db_type: str, credentials: dict):
        self.db_type = db_type
        self.credentials = credentials

        if self.db_type == "bigquery":
            self.db_client = BQClient(
                project_id=self.credentials.get("project_id", None),
                service_account=self.credentials.get("service_account", None),
                credentials_json_env_var=self.credentials.get(
                    "credentials_json_env_var", None
                ),
            )
        else:
            raise Exception(f"Database type {self.db_type} not supported")

    def get_table(self, *args, **kwargs):
        return self.db_client.get_table(*args, **kwargs)

    def list_tables(self, *args, **kwargs):
        return self.db_client.list_tables(*args, **kwargs)

    def get_table_names(self, *args, **kwargs):
        return self.db_client.get_table_names(*args, **kwargs)

    # Defining the method in the client class since the definition
    # may differ between databases
    def is_nested_field(self, field):
        return self.db_client.is_nested_field(field)


class BQClient:
    def __init__(
        self,
        project_id: str,
        service_account: str = None,
        credentials_json_env_var: str = None,
    ):
        bigquery = import_module("google.cloud.bigquery")

        self.project_id = project_id
        if not self.project_id:
            raise ValueError("Project ID is required for BigQuery client")
        self.service_account = service_account

        if credentials_json_env_var and os.getenv(credentials_json_env_var):
            google_auth = import_module("google.auth")
            credentials_json = os.getenv(credentials_json_env_var)
            if not credentials_json:
                raise ValueError(
                    f"Environment variable '{credentials_json_env_var}' not set or empty."
                )

            try:
                credentials_info = json.loads(credentials_json)
            except json.JSONDecodeError:
                raise ValueError(
                    f"Failed to decode JSON from environment variable '{credentials_json_env_var}'."
                )

            credentials, project_id = google_auth.load_credentials_from_dict(
                credentials_info
            )
            self.bq = bigquery.Client(
                project=self.project_id or project_id, credentials=credentials
            )
        elif self.service_account:
            # Create BigQuery client with service account
            self.bq = bigquery.Client.from_service_account_json(
                self.service_account, project=self.project_id
            )
        else:
            # Create BigQuery client with oauth
            self.bq = bigquery.Client(project=self.project_id)

    def get_client(self):
        return self.bq

    def get_table(self, dataset_id: str, table_id: str):
        # Get BigQuery table from API
        bq_table_ref = self.bq.dataset(dataset_id).table(table_id)
        bq_table = self.bq.get_table(bq_table_ref)

        # Create Table instance
        table = Table(name=bq_table.table_id, internal_schema=bq_table.schema)

        # Process schema fields recursively
        self._process_schema_fields(bq_table.schema, table, table.name)
        return table

    def _process_schema_fields(
        self,
        schema,
        table,
        target_dict_path,
        parent_field_path=None,
        parent_field_type=None,
    ):
        for schema_field in schema:
            field_full_path = (
                f"{parent_field_path}.{schema_field.name}"
                if parent_field_path
                else schema_field.name
            )
            field = Field(
                name=schema_field.name,
                internal_type=(
                    schema_field.field_type
                    if not parent_field_type == "ARRAY"
                    else schema_field.internal_type
                ),
                mode=schema_field.mode,
                description=schema_field.description,
                parent_field_name=parent_field_path,
                parent_field_type=parent_field_type,
            )

            if field.is_nested_field or field.is_struct_field or field.is_array_field:
                nested_field_info = {
                    "field_object": field,
                    "nested_schema": {},
                }
                table.add_field_to_schema(target_dict_path, nested_field_info)

                current_parent_field_type = None
                if field.is_nested_field:
                    current_parent_field_type = "RECORD_REPEATED"
                elif field.is_struct_field:
                    current_parent_field_type = "RECORD_NULLABLE"
                elif field.is_array_field:
                    current_parent_field_type = "ARRAY"

                self._process_schema_fields(
                    (
                        schema_field.fields
                        if hasattr(schema_field, "fields")
                        and schema_field.fields
                        and not field.is_array_field
                        else [
                            Field(
                                name=field.name,
                                internal_type=field.internal_type,
                                mode="NULLABLE",
                                description=f"Elements of the array field: {field.name}",
                                parent_field_name=parent_field_path,
                                parent_field_type=parent_field_type,
                            )
                        ]
                    ),
                    table,
                    f"{target_dict_path}.{field.name}",
                    parent_field_path=field_full_path,
                    parent_field_type=current_parent_field_type,
                )
            else:
                table.add_field_to_schema(target_dict_path, field)

    def list_tables(self, dataset_id: str):
        dataset_ref = self.bq.dataset(dataset_id)
        tables = self.bq.list_tables(dataset_ref)
        return [self.get_table(dataset_id, table.table_id) for table in tables]

    def get_table_names(self, dataset_id: str) -> list[str]:
        return [table.table_id for table in self.bq.list_tables(dataset_id)]


class Field:
    def __init__(
        self,
        name: str,
        internal_type: str,
        mode: str,
        description: str,
        parent_field_name: Optional[str] = None,
        parent_field_type: Optional[str] = None,
    ) -> None:
        """Initialisation of the field

        Args:
            name (str): Name of the field
            internal_type (str): Type of the field. Depends on the data warehouse engine.
            mode (str): Mode of the field. In BigQuery, can be NULLABLE, REQUIRED or REPEATED.
            description (str): Description of the field.
        """
        self.name = name
        self.internal_type = internal_type
        self.description = description
        self.mode = mode
        self.parent_field_name = parent_field_name
        self.is_array_field = self.mode == "REPEATED" and self.internal_type != "RECORD"
        self.is_nested_field = (
            self.internal_type == "RECORD" and self.mode == "REPEATED"
        )
        self.is_struct_field = (
            self.internal_type == "RECORD" and self.mode == "NULLABLE"
        )
        self.parent_field_type = parent_field_type

    def __eq__(self, other):
        if not isinstance(other, Field):
            return False
        return (
            self.name == other.name
            and self.internal_type == other.internal_type
            and self.mode == other.mode
            and self.description == other.description
            and self.parent_field_name == other.parent_field_name
            and self.is_nested_field == other.is_nested_field
            and self.is_struct_field == other.is_struct_field
            and self.parent_field_type == other.parent_field_type
        )

    def __hash__(self):
        return hash(
            (
                self.name,
                self.internal_type,
                self.mode,
                self.description,
                self.parent_field_name,
                self.parent_field_type,
            )
        )

    def get_name(self):
        return self.name

    def get_internal_type(self):
        return self.internal_type

    def get_mode(self):
        return self.mode

    def get_description(self):
        return self.description


class Table:
    def __init__(self, name: str, internal_schema) -> None:
        """Initialisation of the table.

        Args:
            name (str): Name of the table in the dbt dataset.
            internal_schema (_type_): Object from the client that contains the table schema.
        """
        self.name = name
        self.description = None
        self.internal_schema = internal_schema
        self.schema = {self.name: {}}

    def __eq__(self, other):
        if not isinstance(other, Table):
            return False

        return self.name == other.name and self.description == other.description

    def add_field_to_schema(
        self, target_dict_path: str, item_to_add: Union[Field, dict]
    ):
        parts = target_dict_path.split(".")
        current_dict = self.schema

        # Traverse the dictionary path. The first part is the table name.
        # Subsequent parts are field names within nested dictionaries.
        for i, part in enumerate(parts):
            if i == 0:  # The table name is the first key
                current_dict = current_dict[part]
            else:
                # Find the Field object that matches the part name in the current_dict's keys
                found_field_key = None
                for key in current_dict.keys():
                    if isinstance(key, Field) and key.name == part:
                        found_field_key = key
                        break
                if found_field_key:
                    current_dict = current_dict[found_field_key]
                else:
                    raise ValueError(
                        f"Path part '{part}' (Field) not found in schema for target_dict_path '{target_dict_path}'"
                    )

        # Add the item to the final dictionary in the path
        if isinstance(item_to_add, Field):
            # For a regular field, add it directly as a key, with an empty dict as its value
            current_dict[item_to_add] = {}
        elif (
            isinstance(item_to_add, dict)
            and "field_object" in item_to_add
            and "nested_schema" in item_to_add
        ):
            # For a nested field (RECORD type), add the Field object as key and its nested_schema (empty dict) as value
            field_obj = item_to_add["field_object"]
            nested_schema = item_to_add["nested_schema"]
            current_dict[field_obj] = nested_schema
        else:
            raise ValueError("Invalid item_to_add type or structure")

    def get_table_name(self):
        return self.name

    def get_internal_schema(self):
        return self.internal_schema

    def get_schema(self):
        return self.schema

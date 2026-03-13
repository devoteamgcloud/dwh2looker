import datetime
import os
from copy import deepcopy

from jinja2 import Environment, FileSystemLoader

from dwh2looker.db_client import db_client as db
from dwh2looker.logger import Logger
from dwh2looker.lookml_generator.config import DEFAULT_TIMEFRAMES, Config
from dwh2looker.lookml_generator.generators import (
    DimensionGenerator,
    DimensionGroupGenerator,
    ExploreGenerator,
    JoinGenerator,
    LookMLFileGenerator,
    NestedFieldHelper,
    RefinedViewGenerator,
    ViewGenerator,
)
from dwh2looker.lookml_generator.models import View
from dwh2looker.lookml_generator.writer import LookMLFileWriter
from dwh2looker.vc_client.vc_client import GithubClient

CONSOLE_LOGGER = Logger().get_logger()


class LookMLGenerator:
    def __init__(
        self,
        db_type: str,
        push_lookml_to_looker: bool = False,
        github_token: str = None,
        github_app: bool = False,
    ):
        self.db_type = db_type
        self.push_lookml_to_looker = push_lookml_to_looker
        self.github_token = github_token
        self.github_app = github_app
        self.config = Config(os.getenv("dwh2looker_CONFIG_FILE"))
        self.primary_key_prefixes = self.config.get_property("primary_key_prefixes", [])
        self.foreign_key_prefixes = self.config.get_property(
            "foreign_key_prefixes", ["fk_"]
        )
        self.business_key_prefixes = self.config.get_property(
            "business_key_prefixes", ["bk_"]
        )
        self.ignore_column_types = self.config.get_property("ignore_column_types", [])
        self.ignore_modes = self.config.get_property("ignore_modes", [])
        self.timeframes = self.config.get_property("timeframes", DEFAULT_TIMEFRAMES)
        self.time_suffixes = self.config.get_property("time_suffixes", [])
        self.capitalize_ids = self.config.get_property("capitalize_ids", False)
        self.hide_foreign_keys = self.config.get_property("hide_foreign_keys", True)
        self.dimension_groups_excluded = self.config.get_property(
            "dimension_groups_excluded", []
        )
        self.tables_env = self.config.get_property(
            "tables_env",
        )
        self.output_dirs = []
        self.existing_refined_views = []
        self.looker_repo_structure = self.config.get_property(
            "looker_repo_structure", {}
        )
        # raise an exception if looker_repo_steruct does not have repo_url when push_lookml_to_looker
        if self.push_lookml_to_looker and not self.looker_repo_structure.get(
            "repo_url"
        ):
            raise ValueError(
                "looker_repo_structure.repo_url is required in the config file when pushing LookML to Looker."
            )
        self.jinja_env = Environment(
            loader=FileSystemLoader(
                os.path.join(os.path.dirname(__file__), "templates")
            ),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self.nested_field_helper = NestedFieldHelper(capitalize_ids=self.capitalize_ids)
        self.dimension_generator = DimensionGenerator(
            db_type=self.db_type,
            capitalize_ids=self.capitalize_ids,
            primary_key_prefixes=self.primary_key_prefixes,
            foreign_key_prefixes=self.foreign_key_prefixes,
            business_key_prefixes=self.business_key_prefixes,
            jinja_env=self.jinja_env,
            nested_field_helper=self.nested_field_helper,
            hide_foreign_keys=self.hide_foreign_keys,
        )
        self.dimension_group_generator = DimensionGroupGenerator(
            timeframes=self.timeframes,
            time_suffixes=self.time_suffixes,
            primary_key_prefixes=self.primary_key_prefixes,
            jinja_env=self.jinja_env,
            nested_field_helper=self.nested_field_helper,
        )
        self.view_generator = ViewGenerator(self.jinja_env)
        self.join_generator = JoinGenerator(self.jinja_env)
        self.explore_generator = ExploreGenerator(
            self.jinja_env,
            explore_view_name_prefixes=self.config.get_property(
                "explore_view_name_prefixes", ["dim_", "fct_"]
            ),
        )
        self.refined_view_generator = RefinedViewGenerator(
            jinja_env=self.jinja_env,
        )
        self.file_writer = LookMLFileWriter()
        self.lookml_file_generator = LookMLFileGenerator(self.jinja_env)
        self.github_client = (
            GithubClient(
                token=self.github_token,
                repo=self.looker_repo_structure.get("repo_url"),
                user_email=self.looker_repo_structure.get("github_user_email"),
                github_app=self.github_app,
                main_branch=self.looker_repo_structure.get("main_branch"),
            )
            if self.push_lookml_to_looker
            else None
        )

        # raise exceptions if github_client is None but push_lookml_to_looke
        if self.push_lookml_to_looker and not self.github_client:
            raise ValueError(
                "GitHub client failed to be initialized. Cannot push LookML to Looker."
            )

    def sort_fields(self, fields: list[db.Field]) -> list[db.Field]:
        pk_fields = [
            field
            for field in fields
            if any(
                field.name.lower().startswith(pk_prefix.lower())
                for pk_prefix in self.primary_key_prefixes
            )
        ]
        fk_fields = [
            field
            for field in fields
            if not any(
                field.name.lower().startswith(pk_prefix.lower())
                for pk_prefix in self.primary_key_prefixes
            )
            and any(
                field.name.lower().startswith(fk_prefix.lower())
                for fk_prefix in self.foreign_key_prefixes
            )
        ]
        bk_fields = [
            field
            for field in fields
            if not any(
                field.name.lower().startswith(pk_prefix.lower())
                for pk_prefix in self.primary_key_prefixes
            )
            and not any(
                field.name.lower().startswith(fk_prefix.lower())
                for fk_prefix in self.foreign_key_prefixes
            )
            and any(
                field.name.lower().startswith(bk_prefix.lower())
                for bk_prefix in self.business_key_prefixes
            )
        ]
        nested_fields = [
            field
            for field in fields
            if isinstance(field, db.Field)
            and (field.is_nested_field or field.is_array_field)
        ]
        other_fields = [
            field
            for field in fields
            if field not in nested_fields + pk_fields + fk_fields + bk_fields
        ]
        return (
            pk_fields
            + bk_fields
            + fk_fields
            + sorted(other_fields, key=lambda x: x.name.lower())
            + nested_fields
        )

    def process_field(self, field: db.Field):
        field_mode = field.mode
        field_type = field.internal_type

        if field_mode is not None and field_mode in self.ignore_modes:
            return

        if self.ignore_column_types and field_type in self.ignore_column_types:
            return

        if field_type in ["DATETIME", "TIMESTAMP", "DATE"] and not any(
            [
                dimension_group_excluded in field.name
                for dimension_group_excluded in self.dimension_groups_excluded
            ]
        ):
            dimension_group = self.dimension_group_generator.create_dimension_group(
                field
            )
            yield self.dimension_group_generator.render(dimension_group)

        else:
            dimension = self.dimension_generator.create_dimension(field)
            yield self.dimension_generator.render(dimension)

    def process_views(
        self,
        schema: dict,
        view_name: str,
        sql_table_name: str = None,
        parent_view_path: str = None,
    ):
        current_view_path = (
            f"{parent_view_path}.{view_name}" if parent_view_path else view_name
        )
        fields_in_schema = schema
        # At the top level, the schema is { 'table_name': { ... fields ...}}
        # so we need to get the inner dictionary.
        # In recursive calls, schema is already { ... fields ... }
        if view_name in schema and isinstance(schema[view_name], dict):
            fields_in_schema = schema[view_name]

        if not fields_in_schema:
            CONSOLE_LOGGER.warning(f"Schema for '{view_name}' not found.")
            return

        fields_to_process_for_view = []
        struct_fields_to_flatten = list(self.sort_fields(fields_in_schema.keys()))

        # Store the initial struct fields that need to be flattened one level deep
        current_level_struct_fields = list(struct_fields_to_flatten)
        struct_fields_to_flatten.clear()  # Clear it, as we are not doing further recursive flattening from this list

        for field in current_level_struct_fields:
            fields_to_process_for_view.append(field)
            if field.is_struct_field:
                sub_fields = self.sort_fields(fields_in_schema[field].keys())
                fields_to_process_for_view.extend(sub_fields)  # Add immediate children

        processed_fields = []
        for field in fields_to_process_for_view:
            if not field.is_struct_field:
                processed_field = self.process_field(field)
                if processed_field:
                    processed_fields.extend(processed_field)

            if field.is_nested_field or field.is_array_field:
                nested_schema = fields_in_schema[field]
                yield from self.process_views(
                    schema=nested_schema,
                    view_name=field.name,
                    parent_view_path=current_view_path,
                )

        if processed_fields:
            view = self.view_generator.create_view(
                view_name=view_name,
                sql_table_name=sql_table_name,
                fields=processed_fields,
                full_view_path=current_view_path,
            )
            yield self.view_generator.render(view), view

    def process_joins(self, view_label: str, views: list[View]):
        joins = []
        for view in views:
            join = self.join_generator.create_join(
                join_name=self.join_generator.get_join_name(view.full_view_path),
                view_label=view_label,
                sql=f"LEFT JOIN UNNEST(${{{self.join_generator.get_sql_join_name(view.full_view_path)}}}) AS {self.join_generator.get_join_name(view.full_view_path)}",
                relationship="one_to_many",
            )
            joins.append(self.join_generator.render(join))
        return joins

    def process_explore(self, views: list[View]):
        if len(views) <= 1:
            return None
        base_view = views.pop(0)
        view_label = self.explore_generator.get_view_label(base_view.full_view_path)
        joins = self.process_joins(view_label, views)

        explore = self.explore_generator.create_explore(
            explore_name=base_view.name,
            view_label=view_label,
            hidden="yes",
            extension="required",
            from_=base_view.name,
            joins=joins,
        )
        return self.explore_generator.render(explore)

    def process_refined_views(self, view_name: str, views: list[View]):
        refined_view = self.refined_view_generator.create_refined_view(
            include=f"/{self.looker_repo_structure.get('base_views').replace('env', f'{self.env}')}{view_name}.view.lkml",
            views=views,
        )
        return self.refined_view_generator.render(refined_view)

    def generate_lookml(
        self,
        table_id: str,
        view_name: str = None,
        override_dataset_id: str = None,
    ):
        if not view_name:
            view_name = table_id

        sql_table_name = (
            f"{self.project_id}.{override_dataset_id}.{table_id}"
            if override_dataset_id
            else f"{self.project_id}.{self.dataset_id}.{table_id}"
        )

        table = self.client.get_table(self.dataset_id, table_id)

        view_outputs = []
        views = []
        for view_output, view in self.process_views(
            schema=table.schema, view_name=table.name, sql_table_name=sql_table_name
        ):
            view_outputs.append(view_output)
            views.append(view)
        views = views[::-1]  # Reverse to have nested views at the end
        views_copy = deepcopy(views)
        view_outputs = view_outputs[::-1]  # Reverse to have nested views at the end
        explore_output = self.process_explore(views)

        lkml_view_output = self.lookml_file_generator.render_lkml_view(
            views=view_outputs, explore=explore_output
        )

        if (
            self.create_refined_views
            and view_name.lower().strip() not in self.existing_refined_views
        ):
            # check the current files in the repo and only add if it does not exist
            refined_view_output = self.process_refined_views(
                view_name=view_name, views=views_copy
            )
            output_dir_path = "refined_views"
            self.file_writer.write_lookml(
                content=refined_view_output,
                file_name=view_name,
                type="layer.view",
                output_dir=output_dir_path,
            )
            output_dir_info = {
                "type": "refined_views",
                "path": output_dir_path,
            }
            if output_dir_info not in self.output_dirs:
                self.output_dirs.append(output_dir_info)

        output_dir_path = f"base_views_{self.env}"
        self.file_writer.write_lookml(
            content=lkml_view_output,
            file_name=view_name,
            type="view",
            output_dir=output_dir_path,
        )
        output_dir_info = {
            "type": "base_views",
            "env": self.env,
            "path": output_dir_path,
        }
        if output_dir_info not in self.output_dirs:
            self.output_dirs.append(output_dir_info)

    def _push_lookml_to_repo(self):
        branch_name = f"{self.looker_repo_structure.get('branch_name')}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
        for output_dir_info in self.output_dirs:
            view_type = output_dir_info["type"]
            env = output_dir_info.get("env")
            local_path = output_dir_info["path"]

            # get remote path from config
            remote_path = self.looker_repo_structure.get(str(view_type)).replace(
                "env", env if env else "env"
            )

            self.github_client.update_files(
                input_dir=local_path,
                output_dir=remote_path,
                target_branch=branch_name,
            )

        self.github_client.create_pull_request(
            base_branch=self.looker_repo_structure.get("main_branch"),
            target_branch=branch_name,
            pr_title="dwh2looker ʕ•ᴥ•ʔ: Automated LookML Update",
            pr_body="This PR was automatically generated by dwh2looker the bear. Please review before merging, bears are bears after all.",
        )

    def generate_batch_lookml_views(self, override_dataset_id: str = None):
        for table_env in self.tables_env:
            self.existing_refined_views = []
            self.create_refined_views = table_env.get("create_refined_views", False)
            if self.push_lookml_to_looker and self.create_refined_views:
                self.existing_refined_views = [
                    file.split(".")[0].lower().strip()
                    for file in self.github_client.get_folder_content(
                        self.looker_repo_structure.get("refined_views")
                    )
                ]
            credentials = {
                "service_account": table_env.get("service_account"),
                "project_id": table_env.get("project_id"),
                "credentials_json_env_var": table_env.get("credentials_json_env_var"),
            }
            self.client = db.DbClient(db_type=self.db_type, credentials=credentials)
            self.dataset_id = table_env.get("dataset_id")
            self.project_id = table_env.get("project_id")
            self.env = table_env.get("env")
            self.exclude_tables = table_env.get("exclude_tables", [])

            tables = self.client.get_table_names(dataset_id=self.dataset_id)

            for table in tables:
                if table in self.exclude_tables:
                    continue
                self.generate_lookml(
                    table_id=table,
                    override_dataset_id=override_dataset_id,
                )

        if self.push_lookml_to_looker:
            self._push_lookml_to_repo()

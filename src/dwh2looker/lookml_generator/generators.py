from typing import Optional

from dwh2looker.db_client import db_client as db
from dwh2looker.lookml_generator.models import (
    Dimension,
    DimensionGroup,
    Explore,
    Join,
    SligroRefinedView,
    View,
)
from jinja2 import Environment

FIELD_TYPE_MAPPING = {
    "bigquery": {
        "INTEGER": "number",
        "INT64": "number",
        "FLOAT": "number",
        "FLOAT64": "number",
        "BIGNUMERIC": "number",
        "NUMERIC": "number",
        "BOOLEAN": "yesno",
        "BOOL": "yesno",
        "TIMESTAMP": "date_time",
        "TIME": "string",
        "DATE": "date",
        "DATETIME": "date_time",
        "STRING": "string",
        "ARRAY": "string",
        "GEOGRAPHY": "string",
        "BYTES": "string",
    }
}


class NestedFieldHelper:
    def __init__(self, capitalize_ids: bool = False):
        self.capitalize_ids = capitalize_ids

    def build_field_name(self, field_name: str):
        # replace dots with spaces
        field_name = field_name.replace(".", " ")
        # remove underscores
        field_name = field_name.replace("_", " ")
        # title case
        field_name = field_name.title()
        if self.capitalize_ids:
            # capitalize any "id" in the field name
            field_name = field_name.replace("Id", "ID")
        return field_name


class DimensionGenerator:
    def __init__(
        self,
        db_type: str,
        capitalize_ids: bool,
        primary_key_prefixes: list,
        jinja_env: Environment,
        nested_field_helper: NestedFieldHelper,
    ):
        self.db_type = db_type
        self.capitalize_ids = capitalize_ids
        self.primary_key_prefixes = primary_key_prefixes
        self.jinja_env = jinja_env
        self.nested_field_helper = nested_field_helper

    def _get_looker_type(self, field: db.Field):
        # Default unknown types to string
        return FIELD_TYPE_MAPPING[self.db_type].get(field.internal_type, "string")

    def create_dimension(self, field: db.Field):
        field_name = field.name
        is_array = field.is_array_field
        is_nested = field.is_nested_field
        field_description = field.description
        field_type = field.internal_type
        field_sql_name = f"${{TABLE}}.{field.name}"
        lookml_type = self._get_looker_type(field)
        pk = None
        convert_ts = None
        datatype = None
        group_label = None
        group_item_label = None
        hidden = None
        full_suggestions = None
        tags = None
        hide_type = None

        if field_type == "DATE":
            datatype = "date"
        elif field_type == "DATETIME":
            convert_ts = "no"
            datatype = "datetime"
        elif field_type == "TIMESTAMP":
            convert_ts = "no"
            datatype = "timestamp"

        field_description = (
            field_description.rstrip().replace('"', "'") if field_description else ""
        )

        if field.parent_field_name:
            if field.parent_field_type == "RECORD_NULLABLE":
                field_sql_name = f"${{TABLE}}.{field.parent_field_name}.{field_name}"
                field_name = (
                    f"{field.parent_field_name.replace('.', '__')}__{field_name}"
                )
            elif field.parent_field_type == "ARRAY":
                field_sql_name = "${TABLE}"

            if (
                not (is_nested or is_array)
                and field.parent_field_type not in ["ARRAY"]
                and not field_name.lower().startswith("fk_")
                and not field_name.lower().startswith("pk_")
            ):
                group_item_label = self.nested_field_helper.build_field_name(field_name)
                group_label = self.nested_field_helper.build_field_name(
                    field.parent_field_name
                )
        if any(
            field_name.lower().startswith(primary_key_prefix.lower())
            for primary_key_prefix in self.primary_key_prefixes
        ):
            pk = "yes"
            hidden = "yes"
        elif field_name.lower().startswith("fk_"):
            hidden = "yes"
        elif field_name.lower().startswith("bk_"):
            hidden = "yes"

        if is_nested or is_array:
            hidden = "yes"
            tags = ["ci: ignore"]
            hide_type = True
        if (
            field.parent_field_type in ["RECORD_REPEATED", "ARRAY"]
            and not (is_nested or is_array)
            and not field_name.lower().startswith("fk_")
            and not field_name.lower().startswith("pk_")
        ):
            full_suggestions = "yes"

        return Dimension(
            name=field_name,
            description=field_description,
            pk=pk,
            hidden=hidden,
            type=lookml_type,
            is_nested=is_nested,
            tags=tags,
            hide_type=hide_type,
            sql=field_sql_name,
            group_label=group_label,
            group_item_label=group_item_label,
            convert_tz=convert_ts,
            datatype=datatype,
            full_suggestions=full_suggestions,
        )

    def render(self, dimension: Dimension):
        template = self.jinja_env.get_template("dimension.lkml.j2")
        return template.render(dimension.model_dump())


class DimensionGroupGenerator:
    def __init__(
        self,
        timeframes: list,
        time_suffixes: list,
        primary_key_prefixes: list,
        jinja_env: Environment,
        nested_field_helper: NestedFieldHelper,
    ):
        self.timeframes = timeframes
        self.time_suffixes = time_suffixes
        self.primary_key_prefixes = primary_key_prefixes
        self.jinja_env = jinja_env
        self.nested_field_helper = nested_field_helper

    def _build_timeframes(self, field_type: str):
        if field_type == "DATE":
            return [tf for tf in self.timeframes if tf != "time"]
        else:
            return self.timeframes

    def create_dimension_group(self, field: db.Field):
        field_name = field.name
        field_sql_name = f"${{TABLE}}.{field.name}"
        field_type = field.internal_type
        field_description = field.description
        hidden = None
        convert_ts = None
        datatype = None
        group_item_label = None
        group_label = None
        full_suggestions = None

        if field_type == "DATE":
            datatype = "date"
        elif field_type == "DATETIME":
            convert_ts = "no"
            datatype = "datetime"
        elif field_type == "TIMESTAMP":
            convert_ts = "no"
            datatype = "timestamp"

        field_description = (
            field_description.rstrip().replace('"', "'") if field_description else ""
        )

        if field.parent_field_name:
            field_sql_name = f"${{TABLE}}.{field.name}"
            group_item_label = self.nested_field_helper.build_field_name(field.name)
            group_label = self.nested_field_helper.build_field_name(
                field.parent_field_name
            )
        if any(
            field_name.lower().startswith(primary_key_prefix.lower())
            for primary_key_prefix in self.primary_key_prefixes
        ):
            hidden = "yes"
        elif field_name.lower().startswith("fk_"):
            hidden = "yes"

        if field.parent_field_type == "RECORD_REPEATED":
            full_suggestions = "yes"

        if len(self.time_suffixes) > 0:
            for s in self.time_suffixes:
                if field_name.endswith(s):
                    field_name = "_".join(field_name.split("_")[:-1])
                    break

        return DimensionGroup(
            name=field_name,
            description=field_description,
            hidden=hidden,
            sql=field_sql_name,
            timeframes=self._build_timeframes(field_type),
            convert_tz=convert_ts,
            datatype=datatype,
            group_item_label=group_item_label,
            group_label=group_label,
            full_suggestions=full_suggestions,
        )

    def render(self, dimension_group: DimensionGroup):
        template = self.jinja_env.get_template("dimension_group.lkml.j2")
        return template.render(dimension_group.model_dump())


class ViewGenerator:
    def __init__(self, jinja_env: Environment):
        self.jinja_env = jinja_env

    def create_view(
        self,
        view_name: str,
        sql_table_name: Optional[str],
        fields: list[str],
        full_view_path: Optional[str] = None,
    ):
        return View(
            name=view_name,
            sql_table_name=sql_table_name if sql_table_name else None,
            fields=fields,
            full_view_path=full_view_path,
        )

    def render(self, view: View):
        template = self.jinja_env.get_template("view.lkml.j2")
        return template.render(view.model_dump())


class ExploreGenerator:
    def __init__(self, jinja_env: Environment):
        self.jinja_env = jinja_env

    def get_view_label(self, view_name: str) -> str:
        if view_name.startswith("dim_") or view_name.startswith("fct_"):
            entity_name = (
                view_name.replace("dim_", "").replace("fct_", "").split("__")[0]
            )
            return entity_name.title().replace("_", " ")
        return view_name

    def create_explore(
        self,
        explore_name: str,
        view_label: Optional[str],
        hidden: Optional[str],
        extension: Optional[str],
        joins: list[str],
        from_: Optional[str] = None,
    ):
        return Explore(
            name=explore_name,
            view_label=view_label,
            hidden=hidden,
            extension=extension,
            from_=from_,
            joins=joins,
        )

    def render(self, explore: Explore):
        template = self.jinja_env.get_template("explore.lkml.j2")
        return template.render(explore.model_dump())


class JoinGenerator:
    def __init__(self, jinja_env: Environment):
        self.jinja_env = jinja_env

    def get_sql_join_name(self, full_view_path: str) -> str:
        if full_view_path.count(".") <= 1:
            return full_view_path

        prefix, suffix = full_view_path.rsplit(".", 1)
        prefix_transformed = prefix.replace(".", "__")
        return f"{prefix_transformed}.{suffix}"

    def get_join_name(self, full_view_path: str) -> str:
        return full_view_path.replace(".", "__")

    def create_join(
        self,
        join_name: str,
        view_label: Optional[str],
        sql: str,
        relationship: str,
    ):
        return Join(
            name=join_name,
            view_label=view_label,
            sql=sql,
            relationship=relationship,
        )

    def render(self, join: Join):
        template = self.jinja_env.get_template("join.lkml.j2")
        return template.render(join.model_dump())


class SligroRefinedViewGenerator:
    def __init__(self, jinja_env: Environment):
        self.jinja_env = jinja_env

    def create_refined_view(
        self,
        include: str,
        views: list[View],
    ):
        return SligroRefinedView(
            include=include,
            views=views,
        )

    def render(self, refined_view: SligroRefinedView):
        template = self.jinja_env.get_template("refined_view.j2")
        return template.render(refined_view.model_dump())


class LookMLFileGenerator:
    def __init__(self, jinja_env: Environment):
        self.jinja_env = jinja_env

    def render_lkml_view(self, views: list[str], explore: str = None):
        template = self.jinja_env.get_template("lookml_view.j2")
        return template.render(explore=explore, views=views)

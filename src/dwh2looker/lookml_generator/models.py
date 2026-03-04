from typing import List, Optional

from pydantic import BaseModel


class Dimension(BaseModel):
    name: str
    description: str
    sql: str
    type: str
    is_nested: Optional[bool] = None
    hide_type: Optional[bool] = None
    pk: Optional[str] = None
    hidden: Optional[str] = None
    group_label: Optional[str] = None
    group_item_label: Optional[str] = None
    convert_tz: Optional[str] = None
    datatype: Optional[str] = None
    full_suggestions: Optional[str] = None
    tags: Optional[List[str]] = None


class DimensionGroup(BaseModel):
    name: str
    description: str
    sql: str
    timeframes: List[str]
    hidden: Optional[str] = None
    group_label: Optional[str] = None
    group_item_label: Optional[str] = None
    convert_tz: Optional[str] = None
    datatype: Optional[str] = None
    full_suggestions: Optional[str] = None


class View(BaseModel):
    name: str
    sql_table_name: Optional[str]
    full_view_path: Optional[str] = None
    fields: List[str]


class Explore(BaseModel):
    name: str
    view_label: str
    hidden: Optional[str] = None
    extension: Optional[str] = None
    from_: Optional[str] = None
    joins: List[str]


class Join(BaseModel):
    name: str
    view_label: Optional[str] = None
    sql: str
    relationship: str = None


class SligroRefinedView(BaseModel):
    include: Optional[str] = None
    views: List[View]

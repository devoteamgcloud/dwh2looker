import json
import os

DEFAULT_TIMEFRAMES = [
    "raw",
    "time",
    "day_of_year",
    "week",
    "week_of_year",
    "month",
    "month_name",
    "month_num",
    "quarter",
    "year",
]


class Config:
    def __init__(self, config_file_path=None):
        self._config_file_path = config_file_path
        self._custom_config = self._read_custom_config()

    def _read_custom_config(self):
        if self._config_file_path and os.path.exists(self._config_file_path):
            try:
                with open(self._config_file_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                raise Exception(f"Invalid config file path: {e}") from e
        return {}

    def get_property(self, property_name, default_value=None):
        return self._custom_config.get(property_name, default_value)
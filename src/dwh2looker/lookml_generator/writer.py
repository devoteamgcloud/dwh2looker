import os

from dwh2looker.logger import Logger

CONSOLE_LOGGER = Logger().get_logger()


class LookMLFileWriter:
    def write_lookml(
        self, content: str, file_name: str, type: str, output_dir: str = None
    ):
        lookml_file_path = f"{file_name}.{type}.lkml"
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            lookml_file_path = os.path.join(output_dir, lookml_file_path)

        with open(lookml_file_path, "w") as file:
            file.write(content)

        CONSOLE_LOGGER.info(f"LookML {type} written to {lookml_file_path}")

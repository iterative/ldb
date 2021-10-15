from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

from tomlkit import document, dumps, parse
from tomlkit.toml_document import TOMLDocument

from ldb.path import (
    CONFIG_PATH_FUNCTIONS,
    DEFAULT_CONFIG_TYPES,
    ConfigType,
    get_first_config_path,
)


def load_path(path: Path) -> TOMLDocument:
    if path.exists():
        with path.open() as file:
            return parse(file.read())
    return document()


def load_first_path(
    config_types=DEFAULT_CONFIG_TYPES,
) -> Optional[TOMLDocument]:
    config_path = get_first_config_path(config_types)
    if config_path is None:
        return None
    with config_path.open() as file:
        config_str = file.read()
    return parse(config_str)


@contextmanager
def edit_path(path: Path) -> Generator[TOMLDocument, None, None]:
    config = load_path(path)
    yield config
    config_str = dumps(config)
    with path.open("w") as file:
        file.write(config_str)


@contextmanager
def edit(
    config_type=ConfigType.INSTANCE,
) -> Generator[TOMLDocument, None, None]:
    with edit_path(CONFIG_PATH_FUNCTIONS[config_type]()) as config:
        yield config

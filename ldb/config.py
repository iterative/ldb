from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

from tomlkit import document, dumps, parse
from tomlkit.toml_document import TOMLDocument

from ldb.path import DEFAULT_CONFIG_TYPES, ConfigType, Filename, get_config_dir


def load_from_path(path: Path) -> TOMLDocument:
    with path.open() as file:
        config_str = file.read()
    return parse(config_str)


def save_to_path(config: TOMLDocument, path: Path):
    config_str = dumps(config)
    with path.open("w") as file:
        file.write(config_str)


def load_first_path(
    config_types=DEFAULT_CONFIG_TYPES,
) -> Optional[TOMLDocument]:
    for config_type in config_types:
        config_dir = get_config_dir(config_type)
        if config_dir is not None:
            path = config_dir / Filename.CONFIG
            try:
                return load_from_path(path)
            except FileNotFoundError:
                pass
    return None


@contextmanager
def edit_path(path: Path) -> Generator[TOMLDocument, None, None]:
    try:
        config = load_from_path(path)
    except FileNotFoundError:
        config = document()
    yield config
    save_to_path(config, path)


@contextmanager
def edit(
    config_type=ConfigType.INSTANCE,
) -> Generator[TOMLDocument, None, None]:
    with edit_path(get_config_dir(config_type) / Filename.CONFIG) as config:
        yield config

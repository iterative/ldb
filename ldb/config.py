import os
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Dict, Generator, Optional

from appdirs import site_config_dir, user_config_dir
from tomlkit import document, dumps, parse
from tomlkit.exceptions import NonExistentKey
from tomlkit.toml_document import TOMLDocument

from ldb.app_info import APP_AUTHOR, APP_NAME
from ldb.env import Env
from ldb.exceptions import LDBException, LDBInstanceNotFoundError
from ldb.path import DirName, Filename


class ConfigType:
    INSTANCE = "INSTANCE"
    USER = "USER"
    SYSTEM = "SYSTEM"


DEFAULT_CONFIG_TYPES = (
    ConfigType.INSTANCE,
    ConfigType.USER,
    ConfigType.SYSTEM,
)
GLOBAL_CONFIG_TYPES = (
    ConfigType.USER,
    ConfigType.SYSTEM,
)


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
def edit(path: Path) -> Generator[TOMLDocument, None, None]:
    try:
        config = load_from_path(path)
    except FileNotFoundError:
        config = document()
    yield config
    save_to_path(config, path)


def get_ldb_dir() -> Path:
    """Find the directory in which `.ldb/` will be created."""
    if Env.LDB_DIR in os.environ:
        return Path(os.environ[Env.LDB_DIR])
    config = load_first_path(GLOBAL_CONFIG_TYPES)
    if config is not None:
        try:
            ldb_dir_str = config["core"]["ldb_dir"]
        except NonExistentKey:
            pass
        else:
            ldb_dir_path = Path(ldb_dir_str).expanduser()
            if ldb_dir_path.is_absolute():
                return ldb_dir_path
            raise LDBException(
                "Found relative path for core.ldb_dir: {repr(ldb_dir_str)}"
                "Paths in LDB config must be absolute",
            )
    return Path.home() / DirName.LDB


def get_default_instance_dir() -> Path:
    return Path.home() / DirName.LDB


def get_instance_config_dir() -> Optional[Path]:
    try:
        return get_ldb_dir()
    except LDBInstanceNotFoundError:
        return None


def get_default_config_dir() -> Path:
    return get_default_instance_dir()


def get_user_config_dir() -> Optional[Path]:
    try:
        config_dir = user_config_dir(APP_NAME, APP_AUTHOR)
    except OSError:
        return None
    return Path(config_dir)


def get_system_config_dir() -> Optional[Path]:
    try:
        config_dir = site_config_dir(APP_NAME, APP_AUTHOR)
    except OSError:
        return None
    return Path(config_dir)


CONFIG_DIR_FUNCTIONS: Dict[str, Callable[[], Optional[Path]]] = {
    ConfigType.INSTANCE: get_instance_config_dir,
    ConfigType.USER: get_user_config_dir,
    ConfigType.SYSTEM: get_system_config_dir,
}


def get_config_dir(config_type):
    return CONFIG_DIR_FUNCTIONS[config_type]()

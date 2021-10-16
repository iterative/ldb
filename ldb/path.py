import os
from pathlib import Path
from typing import Callable, Dict, Optional

from appdirs import site_config_dir, user_config_dir

from ldb.app_info import APP_AUTHOR, APP_NAME
from ldb.exceptions import LDBInstanceNotFoundError

INSTANCE_DIR_NAME = ".ldb"
CONFIG_FILENAME = "config"


class DirName:
    LDB = ".ldb"
    DATA_OBJECT_INFO = "data_object_info"
    DATASETS = "datasets"
    OBJECTS = "objects"
    ANNOTATIONS = "annotations"
    COLLECTIONS = "collections"
    DATASET_VERSIONS = "dataset_versions"


class Filename:
    CONFIG = "config"
    STORAGE = "storage"


class ConfigType:
    INSTANCE = "INSTANCE"
    DEFAULT = "DEFAULT"
    USER = "USER"
    SYSTEM = "SYSTEM"


DEFAULT_CONFIG_TYPES = (
    ConfigType.INSTANCE,
    ConfigType.USER,
    ConfigType.SYSTEM,
)
INIT_CONFIG_TYPES = (
    ConfigType.DEFAULT,
    ConfigType.USER,
    ConfigType.SYSTEM,
)


def get_default_instance_dir() -> Path:
    return Path.home() / INSTANCE_DIR_NAME


def find_instance_dir(path: Path = None) -> Path:
    """
    Find an ldb instance directory.

    Searches, the given path, each subsequent parent directory, and the
    home directory for a .ldb directory.
    """
    if path is None:
        path = Path.cwd()
    else:
        path = Path(path)
    instance_dir = path / INSTANCE_DIR_NAME
    if instance_dir.is_dir():
        return instance_dir
    curr_path = path
    # Path.is_mount is not implemented on Windows
    # https://docs.python.org/3.7/library/pathlib.html#pathlib.Path.is_mount
    while not os.path.ismount(curr_path):
        curr_path = curr_path.parent
        instance_dir = curr_path / INSTANCE_DIR_NAME
        if instance_dir.is_dir():
            return instance_dir
    instance_dir = get_default_instance_dir()
    if instance_dir.is_dir():
        return instance_dir
    raise LDBInstanceNotFoundError(
        f"No ldb instance found under {repr(os.fspath(path))}, "
        f"under its parents, or at {repr(os.fspath(instance_dir))}",
    )


def get_instance_config_path(path: Path = None) -> Path:
    try:
        instance_dir = find_instance_dir(path=path)
    except LDBInstanceNotFoundError:
        return get_default_config_path()
    return instance_dir / CONFIG_FILENAME


def get_default_config_path() -> Path:
    return get_default_instance_dir() / CONFIG_FILENAME


def get_user_config_path() -> Path:
    return Path(user_config_dir(APP_NAME, APP_AUTHOR)) / CONFIG_FILENAME


def get_system_config_path() -> Path:
    return Path(site_config_dir(APP_NAME, APP_AUTHOR)) / CONFIG_FILENAME


CONFIG_PATH_FUNCTIONS: Dict[str, Callable[[], Path]] = {
    ConfigType.INSTANCE: get_instance_config_path,
    ConfigType.DEFAULT: get_default_config_path,
    ConfigType.USER: get_user_config_path,
    ConfigType.SYSTEM: get_system_config_path,
}


def get_first_config_path(config_types=DEFAULT_CONFIG_TYPES) -> Optional[Path]:
    for config_type in config_types:
        path = CONFIG_PATH_FUNCTIONS[config_type]()
        if path.is_file():
            return path
    return None

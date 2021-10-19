import os
from pathlib import Path
from typing import Callable, Dict, Optional

from appdirs import site_config_dir, user_config_dir

from ldb.app_info import APP_AUTHOR, APP_NAME
from ldb.exceptions import LDBInstanceNotFoundError


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
    ConfigType.DEFAULT,
    ConfigType.USER,
    ConfigType.SYSTEM,
)
GLOBAL_CONFIG_TYPES = (
    ConfigType.DEFAULT,
    ConfigType.USER,
    ConfigType.SYSTEM,
)


def get_default_instance_dir() -> Path:
    return Path.home() / DirName.LDB


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
    instance_dir = path / DirName.LDB
    if instance_dir.is_dir():
        return instance_dir
    curr_path = path
    # Path.is_mount is not implemented on Windows
    # https://docs.python.org/3.7/library/pathlib.html#pathlib.Path.is_mount
    while not os.path.ismount(curr_path):
        curr_path = curr_path.parent
        instance_dir = curr_path / DirName.LDB
        if instance_dir.is_dir():
            return instance_dir
    instance_dir = get_default_instance_dir()
    if instance_dir.is_dir():
        return instance_dir
    raise LDBInstanceNotFoundError(
        f"No ldb instance found under {repr(os.fspath(path))}, "
        f"under its parents, or at {repr(os.fspath(instance_dir))}",
    )


def get_instance_config_dir() -> Optional[Path]:
    try:
        return find_instance_dir()
    except LDBInstanceNotFoundError:
        return None


def get_default_config_dir() -> Path:
    return get_default_instance_dir()


def get_user_config_dir() -> Path:
    return Path(user_config_dir(APP_NAME, APP_AUTHOR))


def get_system_config_dir() -> Path:
    return Path(site_config_dir(APP_NAME, APP_AUTHOR))


CONFIG_DIR_FUNCTIONS: Dict[str, Callable[[], Optional[Path]]] = {
    ConfigType.INSTANCE: get_instance_config_dir,
    ConfigType.DEFAULT: get_default_config_dir,
    ConfigType.USER: get_user_config_dir,
    ConfigType.SYSTEM: get_system_config_dir,
}


def get_config_dir(config_type):
    return CONFIG_DIR_FUNCTIONS[config_type]()

import os
from pathlib import Path

from tomlkit.exceptions import NonExistentKey

from ldb.config import load_first_path
from ldb.env import Env
from ldb.path import (
    CONFIG_FILENAME,
    INIT_CONFIG_TYPES,
    INSTANCE_DIR_NAME,
    DirName,
)

STORAGE_FILENAME = "storage"


ROOT_DIR_NAMES = (
    DirName.DATA_OBJECT_INFO,
    DirName.DATASETS,
    DirName.OBJECTS,
)
OBJECT_DIR_NAMES = (
    DirName.ANNOTATIONS,
    DirName.COLLECTIONS,
    DirName.DATASET_VERSIONS,
)


def init(path: Path = None):
    if path is None:
        path = find_init_location()
    if path.is_dir() and next(path.iterdir(), None) is not None:
        raise Exception(f"directory not empty: {repr(os.fspath(path))}")
    path.mkdir(parents=True, exist_ok=True)
    for dir_name in ROOT_DIR_NAMES:
        (path / dir_name).mkdir()
    object_dir = path / DirName.OBJECTS
    for dir_name in OBJECT_DIR_NAMES:
        (object_dir / dir_name).mkdir()
    for filepath in (CONFIG_FILENAME, STORAGE_FILENAME):
        (path / filepath).touch()


def find_init_location() -> Path:
    if Env.LDB_DIR in os.environ:
        return Path(os.environ[Env.LDB_DIR])
    config = load_first_path(INIT_CONFIG_TYPES)
    if config is not None:
        try:
            instance_dir = config["core"]["ldb_dir"]
        except NonExistentKey:
            pass
        else:
            return Path(instance_dir)
    return Path.home() / INSTANCE_DIR_NAME

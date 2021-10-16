import os
import shutil
from pathlib import Path

from tomlkit.exceptions import NonExistentKey

from ldb.config import load_first_path
from ldb.env import Env
from ldb.exceptions import LDBException
from ldb.path import CONFIG_FILENAME, INIT_CONFIG_TYPES, DirName

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


def init(path: Path = None, force: bool = False):
    """Create new LDB instance."""
    if path is None:
        path = find_init_location()
    ldb_dir = path / DirName.LDB
    if ldb_dir.is_dir() and next(ldb_dir.iterdir(), None) is not None:
        if force:
            shutil.rmtree(ldb_dir)
        else:
            raise LDBException(
                "An LDB instance already exists at "
                f"{repr(os.fspath(ldb_dir))}",
            )
    ldb_dir.mkdir(parents=True, exist_ok=True)
    for dir_name in ROOT_DIR_NAMES:
        (ldb_dir / dir_name).mkdir()
    object_dir = ldb_dir / DirName.OBJECTS
    for dir_name in OBJECT_DIR_NAMES:
        (object_dir / dir_name).mkdir()
    for filepath in (CONFIG_FILENAME, STORAGE_FILENAME):
        (ldb_dir / filepath).touch()
    print(f"Initialized LDB instance at {repr(os.fspath(ldb_dir))}")


def find_init_location() -> Path:
    """Find the directory in which `.ldb/` will be created."""
    if Env.LDB_ROOT in os.environ:
        return Path(os.environ[Env.LDB_ROOT])
    config = load_first_path(INIT_CONFIG_TYPES)
    if config is not None:
        try:
            instance_dir = config["core"]["ldb_root"]
        except NonExistentKey:
            pass
        else:
            return Path(instance_dir)
    return Path.home()

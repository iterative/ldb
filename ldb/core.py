import os
import shutil
from pathlib import Path

from ldb.config import get_ldb_dir
from ldb.exceptions import LDBException
from ldb.path import DirName

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
LDB_DIR_STRUCTURE = (
    Path(DirName.DATA_OBJECT_INFO),
    Path(DirName.DATASETS),
    Path(DirName.OBJECTS) / DirName.ANNOTATIONS,
    Path(DirName.OBJECTS) / DirName.COLLECTIONS,
    Path(DirName.OBJECTS) / DirName.DATASET_VERSIONS,
)


def init(path: Path = None, force: bool = False) -> Path:
    """Create a new LDB instance."""
    if path is None:
        path = get_ldb_dir()
    path = path.absolute()
    if path.is_dir() and next(path.iterdir(), None) is not None:
        if is_ldb_instance(path):
            if force:
                shutil.rmtree(path)
            else:
                raise LDBException(
                    "An LDB instance already exists at "
                    f"{repr(os.fspath(path))}\n",
                    "Use the --force option to remove it",
                )
        else:
            raise LDBException(
                f"Directory not empty: {repr(os.fspath(path))}\n"
                "To create an LDB instance here, remove directory contents",
            )
    for subdir in LDB_DIR_STRUCTURE:
        (path / subdir).mkdir(parents=True, exist_ok=True)
    print(f"Initialized LDB instance at {repr(os.fspath(path))}")
    return path


def is_ldb_instance(path: Path) -> bool:
    return all((path / subdir).is_dir() for subdir in LDB_DIR_STRUCTURE)

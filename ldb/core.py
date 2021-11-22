import os
import shutil
from pathlib import Path
from typing import Optional

from ldb import config
from ldb.config import get_default_instance_dir, get_ldb_dir
from ldb.exceptions import LDBException, LDBInstanceNotFoundError
from ldb.path import INSTANCE_DIRS, Filename


def init(
    path: Path,
    force: bool = False,
    read_any_cloud_location: bool = False,
) -> Path:
    """Create a new LDB instance."""
    path = Path(os.path.normpath(path))
    if path.is_dir() and next(path.iterdir(), None) is not None:
        if is_ldb_instance(path):
            if force:
                print(
                    "Removing existing LDB instance at "
                    f"{repr(os.fspath(path))}",
                )
                shutil.rmtree(path)
            else:
                raise LDBException(
                    "Initialization failed\n"
                    "An LDB instance already exists at "
                    f"{repr(os.fspath(path))}\n"
                    "Use the --force option to remove it",
                )
        else:
            raise LDBException(
                f"Directory not empty: {repr(os.fspath(path))}\n"
                "To create an LDB instance here, remove directory contents",
            )
    for subdir in INSTANCE_DIRS:
        (path / subdir).mkdir(parents=True)
    with config.edit(get_default_instance_dir() / Filename.CONFIG) as cfg:
        cfg["core"] = {"read_any_cloud_location": read_any_cloud_location}
    print(f"Initialized LDB instance at {repr(os.fspath(path))}")
    return path


def is_ldb_instance(path: Path) -> bool:
    return all((path / subdir).is_dir() for subdir in INSTANCE_DIRS)


def get_ldb_instance(path: Optional[Path] = None) -> Path:
    if path is None:
        path = get_ldb_dir()
    if not is_ldb_instance(path):
        raise LDBInstanceNotFoundError(
            f"No LDB instance at {os.fspath(path)!r}",
        )
    return path

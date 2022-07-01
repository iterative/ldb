import os
import shlex
import shutil
from pathlib import Path
from typing import Optional

from ldb import config
from ldb.config import get_default_instance_dir, get_global_base, get_ldb_dir
from ldb.exceptions import LDBException, LDBInstanceNotFoundError
from ldb.path import INSTANCE_DIRS, Filename, GlobalDir
from ldb.storage import StorageLocation, add_storage


def init(
    path: Path,
    force: bool = False,
    read_any_cloud_location: bool = False,
    auto_index: bool = False,
) -> Path:
    """
    Create a new LDB instance.
    """
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
    with config.edit(path / Filename.CONFIG) as cfg:
        cfg["core"] = {
            "read_any_cloud_location": read_any_cloud_location,
            "auto_index": auto_index,
        }
    print(f"Initialized LDB instance at {repr(os.fspath(path))}")
    return path


def init_quickstart(force: bool = False) -> Path:
    ldb_dir = init(
        get_default_instance_dir(),
        force=force,
        read_any_cloud_location=True,
        auto_index=True,
    )
    add_default_read_add_storage(ldb_dir)
    add_public_data_lakes(ldb_dir)
    return ldb_dir


def add_default_read_add_storage(ldb_dir: Path) -> None:
    path = get_global_base() / GlobalDir.DEFAULT_READ_ADD_STORAGE
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        path_arg = shlex.quote(os.fspath(path))
        raise LDBException(
            f"Unable to create read-add storage location: {path_arg}\n"
            "Ensure it is a writable directory and add it with:\n\n"
            f"  ldb add-storage -a {path_arg}\n",
        ) from exc
    add_storage(
        ldb_dir / Filename.STORAGE,
        StorageLocation(
            path=os.fspath(path),
            protocol="file",
            read_and_add=True,
        ),
    )


def add_public_data_lakes(ldb_dir: Path) -> None:
    add_storage(
        ldb_dir / Filename.STORAGE,
        StorageLocation(
            path="ldb-public/remote",
            protocol="s3",
            read_and_add=False,
            options={"anon": True},
        ),
    )


def is_ldb_instance(path: Path) -> bool:
    return all((path / subdir).is_dir() for subdir in INSTANCE_DIRS)


def get_ldb_instance(path: Optional[Path] = None) -> Path:
    if path is None:
        path = get_ldb_dir()
    if not is_ldb_instance(path):
        raise LDBInstanceNotFoundError(
            f"No LDB instance at {os.fspath(path)!r}\n\n"
            "For instance initialization help, run:\n\n"
            "\tldb init -h\n",
        )
    return path

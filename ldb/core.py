import os
import os.path as osp
import shlex
import shutil
from pathlib import Path
from typing import Optional, Tuple, Type, Union

from funcy.objects import cached_property

from ldb import config
from ldb.config import get_default_instance_dir, get_global_base, get_ldb_dir
from ldb.db.abstract import AbstractDB
from ldb.db.file import FileDB
from ldb.exceptions import LDBException, LDBInstanceNotFoundError
from ldb.path import REQUIRED_INSTANCE_DIRS, Filename, GlobalDir
from ldb.storage import StorageLocation, add_storage


class LDBClient:
    def __init__(self, ldb_dir: Union[str, Path], db_type: str = ""):
        self.ldb_dir = os.fspath(ldb_dir)
        self._db_type = db_type

    @cached_property
    def db_info(self) -> Tuple[str, str, Type[AbstractDB]]:
        if not self._db_type:
            self._db_type = "file"
        if self._db_type == "file":
            return self._db_type, self.ldb_dir, FileDB
        raise ValueError(f"Invalid db type: {self._db_type}")

    @property
    def db_type(self) -> str:
        return self.db_info[0]

    @property
    def db_path(self) -> str:
        return self.db_info[1]

    @cached_property
    def db(self) -> AbstractDB:
        cls: Type[AbstractDB]
        _, path, cls = self.db_info
        return cls(path)


def init(
    path: Path,
    force: bool = False,
    read_any_cloud_location: bool = False,
    auto_index: bool = False,
    db_type: str = "",
) -> Path:
    """
    Create a new LDB instance.
    """
    if not db_type:
        db_type = os.getenv("LDB_DATABASE", "")
    path = Path(os.path.abspath(path))
    if path.is_dir() and next(path.iterdir(), None) is not None:
        if is_ldb_instance(path):
            if force:
                print(f"Removing existing LDB instance at {repr(os.fspath(path))}")
                with os.scandir(path) as scandir_it:
                    entries = list(scandir_it)
                for entry in entries:
                    entry_path = osp.join(path, entry)
                    if entry.is_dir():
                        shutil.rmtree(entry_path)
                    else:
                        os.unlink(entry_path)
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
    client = LDBClient(path, db_type=db_type)
    os.makedirs(osp.dirname(client.db_path), exist_ok=True)
    client.db.init()

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
    return all((path / subdir).is_dir() for subdir in REQUIRED_INSTANCE_DIRS)


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

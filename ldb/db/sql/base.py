import os
from pathlib import Path
from typing import Any, Union

from dvc_objects.fs.base import FileSystem
from dvc_objects.fs.local import LocalFileSystem, localfs
from dvc_objects.obj import Object

from ldb.db.annotation import AnnotationFileSystemDB
from ldb.db.sql.models import get_db_path, get_session
from ldb.typing import JSONDecoded


class BaseSqliteDB(AnnotationFileSystemDB):
    def __init__(self, fs: "FileSystem", path: str, **kwargs):
        assert isinstance(fs, LocalFileSystem)
        super().__init__(fs, path, **kwargs)
        self.session = get_session(path)

    @classmethod
    def from_ldb_dir(
        cls,
        ldb_dir: Union[str, Path],
        **kwargs: Any,
    ):
        return cls(
            localfs,
            get_db_path(os.fspath(ldb_dir)),
            **kwargs,
        )

    def oid_to_path(self, oid: str) -> str:
        raise NotImplementedError

    def add_obj(self, obj) -> None:
        raise NotImplementedError

    def get_obj(self, oid: str):
        raise NotImplementedError

    def get_part(self, obj_ref: Object, name: str) -> JSONDecoded:
        raise NotImplementedError

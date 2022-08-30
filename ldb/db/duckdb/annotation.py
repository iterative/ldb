import json
import os
from pathlib import Path
from typing import Any, Union, cast
from dvc_objects.fs.base import FileSystem

from dvc_objects.fs.local import LocalFileSystem, localfs
from dvc_objects.obj import Object

from ldb.db.duckdb.connection import get_db_path, get_session
from ldb.db.sql import models
from ldb.db.annotation import AnnotationDB
from ldb.objects.annotation import Annotation
from ldb.typing import JSONDecoded, JSONObject


class AnnotationDuckDB(AnnotationDB):
    def __init__(self, fs: "FileSystem", path: str, **kwargs):
        assert isinstance(fs, LocalFileSystem)
        super().__init__(fs, path, **kwargs)
        self.session = get_session(path)

    @classmethod
    def from_ldb_dir(
        cls,
        ldb_dir: Union[str, Path],
        **kwargs: Any,
    ) -> "AnnotationDB":
        return cls(
            localfs,
            get_db_path(os.fspath(ldb_dir)),
            **kwargs,
        )

    def oid_to_path(self, oid: str) -> str:
        raise NotImplementedError

    def add_obj(self, obj: Annotation) -> None:
        self.session.add(
            models.Annotation(value=obj.value, meta=obj.meta, id=obj.oid),
        )
        self.session.commit()

    def get_obj(self, oid: str) -> Annotation:
        db_obj = self.session.query(models.Annotation).filter(models.Annotation.id.is_(oid)).one()
        return Annotation(
            value=db_obj.value,
            meta=db_obj.meta,
            oid=db_obj.id,
        )

    def get_part(self, obj_ref: Object, name: str) -> JSONDecoded:
        raise NotImplementedError

    def get_value(self, oid: str) -> JSONDecoded:
        return self.session.query(models.Annotation.value).filter(models.Annotation.id == oid).one()

    def get_meta(self, oid: str) -> JSONDecoded:
        return self.session.query(models.Annotation.meta).filter(models.Annotation.id == oid).one()

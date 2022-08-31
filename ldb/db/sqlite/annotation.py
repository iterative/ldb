import os
from pathlib import Path
from typing import Any, Union

from dvc_objects.fs.base import FileSystem
from dvc_objects.fs.local import LocalFileSystem, localfs
from dvc_objects.obj import Object
from sqlalchemy.exc import DBAPIError, NoResultFound

from ldb.db.annotation import AnnotationFileSystemDB
from ldb.db.sql import models
from ldb.db.sql.base import BaseSqliteDB
from ldb.db.sql.models import get_db_path, get_session
from ldb.objects.annotation import Annotation
from ldb.typing import JSONDecoded


class AnnotationSqliteDB(BaseSqliteDB, AnnotationFileSystemDB):
    def add_obj(self, obj: Annotation) -> None:
        assert obj.oid
        self.session.add(
            models.Annotation(value=obj.value, meta=obj.meta, id=obj.oid),
        )
        try:
            self.session.commit()
        except:
            self.session.rollback()

    def get_obj(self, oid: str) -> Annotation:
        try:
            db_obj = (
                self.session.query(models.Annotation)
                .filter(models.Annotation.id == oid)
                .one()
            )
        except NoResultFound as e:
            raise NoResultFound(oid) from e
        return Annotation(
            value=db_obj.value,
            meta=db_obj.meta,
            oid=db_obj.id,
        )

    def get_value(self, oid: str) -> JSONDecoded:
        if not oid:
            return None
        return (
            self.session.query(models.Annotation.value)
            .filter(models.Annotation.id == oid)
            .one()[0]
        )

    def get_value_multi(self, oids):
        return {
            i: v
            for i, v in self.session.query(
                models.Annotation.id,
                models.Annotation.value,
            )
            .filter(models.Annotation.id.in_(oids))
            .all()
        }

    def get_value_all(self):
        return {
            i: v
            for i, v in self.session.query(
                models.Annotation.id,
                models.Annotation.value,
            ).all()
        }

    def get_meta(self, oid: str) -> JSONDecoded:
        if not oid:
            return None
        return (
            self.session.query(models.Annotation.meta)
            .filter(models.Annotation.id == oid)
            .one()[0]
        )

import json

from sqlalchemy.exc import NoResultFound

from ldb.db.sql import models
from ldb.db.sqlite.annotation import AnnotationSqliteDB
from ldb.objects.annotation import Annotation
from ldb.typing import JSONDecoded


class AnnotationDuckDB(AnnotationSqliteDB):
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
            value=json.loads(db_obj.value),
            meta=json.loads(db_obj.meta),
            oid=db_obj.id,
        )

    def get_value(self, oid: str) -> JSONDecoded:
        return json.loads(
            self.session.query(models.Annotation.value)
            .filter(models.Annotation.id == oid)
            .one()[0],
        )

    def get_value_multi(self, oids):
        return {
            i: json.loads(v)
            for i, v in self.session.query(
                models.Annotation.id, models.Annotation.value,
            )
            .filter(models.Annotation.id.in_(oids))
            .all()
        }

    def get_value_all(self):
        return {
            i: json.loads(v)
            for i, v in self.session.query(
                models.Annotation.id, models.Annotation.value,
            ).all()
        }

    def get_meta(self, oid: str) -> JSONDecoded:
        return json.loads(
            self.session.query(models.Annotation.meta)
            .filter(models.Annotation.id == oid)
            .one()[0],
        )

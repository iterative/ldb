
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError, NoResultFound

from ldb.db.data_object import DataObjectFileSystemDB
from ldb.db.sql import models
from ldb.db.sql.base import BaseSqliteDB
from ldb.objects.data_object import DataObjectMeta, PairMeta


class DataObjectSqliteDB(BaseSqliteDB, DataObjectFileSystemDB):
    def add_meta(self, obj: DataObjectMeta) -> None:
        m = models.DataObjectMeta
        meta = m(id=obj.oid, meta=obj.data)
        num = (
            self.session.query(m)
            .filter(m.id == meta.id)
            .update({"meta": meta.meta})
        )
        if not num:
            self.session.add(meta)
        try:
            self.session.commit()
        except DBAPIError:
            self.session.rollback()
            self.session.query(m).filter(m.id == meta.id).update(
                {"meta": meta.meta},
            )
            self.session.commit()
        path_obj = models.DataObjectPath(id=obj.oid, path=obj.data["fs"]["path"])
        self.session.add(path_obj)
        try:
            self.session.commit()
        except DBAPIError:
            self.session.rollback()

    def get_meta(self, oid: str):
        try:
            db_obj = (
                self.session.query(models.DataObjectMeta)
                .filter(models.DataObjectMeta.id == oid)
                .one()
            )
        except NoResultFound as e:
            raise NoResultFound(oid) from e
        return db_obj.meta

    def get_meta_multi(self, oids):
        raise NotImplementedError

    def add_pair_meta(self, obj: PairMeta) -> None:
        m = models.DataObjectAnnotation
        meta = m(id=obj.oid, annot_id=obj.annot_oid, value=obj.data)
        num = (
            self.session.query(m)
            .filter(m.id == meta.id, m.annot_id == meta.annot_id)
            .update({"value": meta.value})
        )
        if not num:
            self.session.add(meta)
        try:
            self.session.commit()
        except DBAPIError:
            self.session.rollback()
            num = (
                self.session.query(m)
                .filter(m.id == meta.id, m.annot_id == meta.annot_id)
                .update({"value": meta.value})
            )
            self.session.commit()

    def get_pair_meta(self, oid: str, annot_id: str):
        try:
            db_obj = (
                self.session.query(models.DataObjectAnnotation)
                .filter(
                    models.DataObjectAnnotation.id == oid,
                    models.DataObjectAnnotation.annot_id == annot_id,
                )
                .one()
            )
        except NoResultFound as e:
            raise NoResultFound((oid, annot_id)) from e
        return db_obj.value

    def get_pair_meta_multi(self, oid_pairs):
        raise NotImplementedError

    def add_current_annot(self, oid: str, annot_id: str):
        m = models.DataObjectCurrentAnnot
        db_obj = m(id=oid, current_annotation=annot_id)
        num = (
            self.session.query(m)
            .filter(m.id == db_obj.id)
            .update({"current_annotation": db_obj.current_annotation})
        )
        if not num:
            self.session.add(db_obj)
        try:
            self.session.commit()
        except DBAPIError:
            self.session.rollback()
            num = (
                self.session.query(m)
                .filter(m.id == db_obj.id)
                .update({"current_annotation": db_obj.current_annotation})
            )
            self.session.commit()

    def get_collection_members(self, *args):
        m = models.DataObjectCurrentAnnot
        return dict(self.session.query(m.id, m.current_annotation))

    def ensure_all_ids_exist(self, oids):
        num = (
            self.session.query(models.DataObjectMeta.id)
            .filter(models.DataObjectMeta.id.in_(oids))
            .count()
        )
        if num != len(oids):
            ids = {
                i[0]
                for i in self.session.query(models.DataObjectMeta.id).filter(
                    models.DataObjectMeta.id.in_(oids),
                )
            }
            for i in oids:
                if i not in ids:
                    raise Exception(f"missing data object {i}")
            raise Exception("missing data object")

    def path_regex(self, pattern: str, oids=None):
        stmt = (
            select(models.DataObjectPath.id)
            .where(
                models.DataObjectPath.path.regexp_match(pattern),
                models.DataObjectPath.id.in_(oids),
            )
        )
        return [
            i[0] for i in self.session.execute(stmt).all()
        ]

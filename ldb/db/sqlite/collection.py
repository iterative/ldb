import json
import os
from pathlib import Path
from typing import Any, Union

from dvc_objects.fs.local import LocalFileSystem

from ldb.db.collection import CollectionFileSystemDB
from ldb.db.sqlite.data_object import DataObjectSqliteDB
from ldb.objects.collection import CollectionObject
from ldb.path import InstanceDir


class CollectionSqliteDB(CollectionFileSystemDB):
    # TODO use sql tables for collections
    @classmethod
    def from_ldb_dir(
        cls,
        ldb_dir: Union[str, Path],
        **kwargs: Any,
    ) -> "CollectionDB":
        return cls(
            LocalFileSystem(),
            os.path.join(ldb_dir, InstanceDir.COLLECTIONS),
            **kwargs,
        )

    def add_obj(self, obj: CollectionObject) -> None:
        self.add_bytes(obj.oid, obj.bytes)

    def get_obj(self, oid: str) -> CollectionObject:
        obj_ref = self.get(oid)
        with obj_ref.fs.open(obj_ref.path, "r") as file:
            contents = file.read()
        obj = CollectionObject(json.loads(contents))
        obj.oid = obj_ref.oid
        return obj

    def get_root(self, ldb_dir):
        db = DataObjectSqliteDB.from_ldb_dir(ldb_dir)
        return CollectionObject(db.get_collection_members(ldb_dir))

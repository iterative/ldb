import json
import os
from pathlib import Path
from typing import Any, Union

from dvc_objects.fs.local import LocalFileSystem

from ldb.db.obj import ObjectDB
from ldb.objects.collection import CollectionObject
from ldb.path import InstanceDir


class CollectionDB(ObjectDB):
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


class DuckDBCollectionDB(ObjectDB):
    @classmethod
    def from_ldb_dir(
        cls,
        ldb_dir: Union[str, Path],
        **kwargs: Any,
    ) -> "DuckDBCollectionDB":
        return cls(
            LocalFileSystem(),
            os.path.join(ldb_dir, "duckdb", "index.duckdb"),
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

import json
import os
from pathlib import Path
from typing import Any, Union

from dvc_objects.fs.local import localfs
from dvc_objects.obj import Object

from ldb.db.obj import ObjectDB
from ldb.objects.data_object import DataObjectMeta, PairMeta
from ldb.path import InstanceDir
from ldb.typing import JSONDecoded


class DataObjectFileSystemDB(ObjectDB):
    @classmethod
    def from_ldb_dir(
        cls,
        ldb_dir: Union[str, Path],
        **kwargs: Any,
    ):
        return cls(
            localfs,
            os.path.join(ldb_dir, InstanceDir.DATA_OBJECT_INFO),
            **kwargs,
        )

    def oid_to_path(self, oid: str) -> str:
        oid, *parts = oid.split(".")
        return self.fs.path.join(  # type: ignore[no-any-return]
            self.path,
            *self._oid_parts(oid),
            *parts,
        )

    def add_obj(self, obj: Object) -> None:
        raise NotImplementedError

    def get_obj(self, oid: str):
        raise NotImplementedError

    def get_part(self, obj_ref: Object, *parts: str) -> str:
        with obj_ref.fs.open(
            obj_ref.fs.path.join(obj_ref.path, *parts),
            "r",
        ) as file:
            data = file.read()
        return data

    def add_meta(self, obj: DataObjectMeta) -> None:
        self.add_bytes(f"{obj.oid}.meta", json.dumps(obj.data).encode())

    def get_meta(self, oid: str):
        return json.loads(self.get_part(self.get(oid), "meta"))

    def get_meta_multi(self, oids):
        return {i: self.get_meta(i) for i in oids}

    def add_pair_meta(self, obj: PairMeta) -> None:
        self.add_bytes(
            f"{obj.oid}.annotations.{obj.annot_oid}",
            json.dumps(obj.data).encode(),
        )

    def get_pair_meta(self, oid: str, annot_id: str):
        return json.loads(
            self.get_part(self.get(oid), "annotations", annot_id),
        )

    def get_pair_meta_multi(self, oid_pairs):
        return {(i, a): self.get_pair_meta(i, a) for i, a in oid_pairs}

import json
import os
from pathlib import Path
from typing import Any, Union

from dvc_objects.fs.local import localfs
from dvc_objects.obj import Object

from ldb.db.obj import ObjectDB
from ldb.exceptions import DataObjectNotFoundError
from ldb.objects.data_object import DataObjectMeta, PairMeta
from ldb.path import InstanceDir
from ldb.utils import DATA_OBJ_ID_PREFIX


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

    def add_current_annot(self, oid: str, annot_id: str):
        self.add_bytes(
            f"{oid}.current",
            annot_id.encode(),
        )

    def get_collection_members(self):
        result = {}
        fs = self.fs
        for p1 in fs.ls(self.path):
            for p2 in fs.ls(p1):
                annot_path = fs.path.join(p1, p2, "current")
                a, b = fs.path.parts(annot_path)[-3:-1]
                oid = a + b
                try:
                    with fs.open(annot_path) as f:
                        annot_id = f.read()
                except Exception:
                    annot_id = ""
                result[oid] = annot_id
        return result

    def ensure_all_ids_exist(self, oids):
        for p1 in self.fs.ls(self.path):
            for p2 in self.fs.ls(p1):
                a, b = self.fs.path.parts(p2)[-2:]
                oid = a + b
                if oid not in oids:
                    raise DataObjectNotFoundError(
                        f"Data object not found: {DATA_OBJ_ID_PREFIX}{oid}",
                    )
import json
import os
from pathlib import Path
from typing import Any, Union, cast

from dvc_objects.fs.local import localfs
from dvc_objects.obj import Object

from ldb.db.obj import ObjectDB
from ldb.objects.annotation import Annotation
from ldb.path import InstanceDir
from ldb.typing import JSONDecoded, JSONObject


class AnnotationFileSystemDB(ObjectDB):
    @classmethod
    def from_ldb_dir(
        cls,
        ldb_dir: Union[str, Path],
        **kwargs: Any,
    ) -> "AnnotationFileSystemDB":
        return cls(
            localfs,
            os.path.join(ldb_dir, InstanceDir.ANNOTATIONS),
            **kwargs,
        )

    def oid_to_path(self, oid: str) -> str:
        oid, *parts = oid.split(".")
        return self.fs.path.join(  # type: ignore[no-any-return]
            self.path,
            *self._oid_parts(oid),
            *parts,
        )

    def add_obj(self, obj: Annotation) -> None:
        self.add_bytes(f"{obj.oid}.user", obj.value_bytes)
        self.add_bytes(f"{obj.oid}.ldb", obj.meta_bytes)

    def get_obj(self, oid: str) -> Annotation:
        obj_ref = self.get(oid)
        obj = Annotation(
            self.get_part(obj_ref, "user"),
            cast(JSONObject, self.get_part(obj_ref, "ldb")),
        )
        obj.oid = oid
        return obj

    def get_part(self, obj_ref: Object, name: str) -> JSONDecoded:
        with obj_ref.fs.open(
            obj_ref.fs.path.join(obj_ref.path, name),
            "r",
        ) as file:
            data = file.read()
        return cast(JSONDecoded, json.loads(data))

    def get_value(self, oid: str) -> JSONDecoded:
        return self.get_part(self.get(oid), "user") if oid else None

    def get_value_multi(self, oids):
        return {i: self.get_value(i) for i in oids}

    def get_value_all(self):
        raise NotImplementedError

    def get_meta(self, oid: str) -> JSONDecoded:
        return self.get_part(self.get(oid), "ldb") if oid else None

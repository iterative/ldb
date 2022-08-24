import json
import os
from pathlib import Path
from typing import Any, Union

from dvc_objects.fs.local import LocalFileSystem

from ldb.dataset import DatasetVersion
from ldb.db.obj import ObjectDB
from ldb.path import InstanceDir


class DatasetVersionDB(ObjectDB):
    @classmethod
    def from_ldb_dir(
        cls,
        ldb_dir: Union[str, Path],
        **kwargs: Any,
    ) -> "DatasetVersionDB":
        return cls(
            LocalFileSystem(),
            os.path.join(ldb_dir, InstanceDir.DATASET_VERSIONS),
            **kwargs,
        )

    def add_obj(self, obj: DatasetVersion) -> None:
        self.add_bytes(obj.oid, obj.bytes)

    def get_obj(self, oid: str) -> DatasetVersion:
        obj_ref = self.get(oid)
        with obj_ref.fs.open(obj_ref.path, "r") as file:
            contents = file.read()
        obj = DatasetVersion.parse(json.loads(contents))
        obj.oid = obj_ref.oid
        return obj

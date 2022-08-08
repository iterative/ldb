import json
import os
from pathlib import Path
from typing import Any, Tuple, Union

from dvc_objects.fs.local import LocalFileSystem

from ldb.db.obj import ObjectDB
from ldb.exceptions import DatasetNotFoundError
from ldb.objects.dataset import Dataset
from ldb.path import InstanceDir


class DatasetDB(ObjectDB):
    @classmethod
    def from_ldb_dir(
        cls,
        ldb_dir: Union[str, Path],
        **kwargs: Any,
    ) -> "DatasetDB":
        return cls(
            LocalFileSystem(),
            os.path.join(ldb_dir, InstanceDir.DATASETS),
            **kwargs,
        )

    def _oid_parts(self, oid: str) -> Tuple[str, ...]:
        return (oid,)

    def add_obj(self, obj: Dataset) -> None:
        self.add_bytes(obj.oid, obj.bytes)

    def get_obj(self, oid: str) -> Dataset:
        obj_ref = self.get(oid)
        try:
            with obj_ref.fs.open(obj_ref.path, "r") as file:
                contents = file.read()
        except FileNotFoundError as exc:
            raise DatasetNotFoundError(
                f"Dataset not found with name {oid!r}",
            ) from exc
        obj = Dataset.parse(json.loads(contents))
        obj.oid = obj_ref.oid
        return obj

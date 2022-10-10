import json
import os
import os.path as osp
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Optional, Tuple, cast

from ldb.db.abstract import (
    AbstractDB,
    AnnotationRecord,
    DataObjectAnnotationRecord,
    DataObjectMetaRecord,
)
from ldb.exceptions import DataObjectNotFoundError
from ldb.path import INSTANCE_DIRS, InstanceDir
from ldb.utils import DATA_OBJ_ID_PREFIX, load_data_file, write_data_file

if TYPE_CHECKING:
    from ldb.index.utils import AnnotationMeta  # noqa: F401
    from ldb.index.utils import DataObjectMeta as DataObjectMetaT  # noqa: F401


class FileDB(AbstractDB):
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.ldb_dir = path
        self.annotation_dir = osp.join(self.ldb_dir, InstanceDir.ANNOTATIONS)
        self.data_object_dir = osp.join(self.ldb_dir, InstanceDir.DATA_OBJECT_INFO)

    @staticmethod
    def oid_parts(id: str) -> Tuple[str, str]:
        return id[:3], id[3:]

    def init(self) -> None:
        for subdir in INSTANCE_DIRS:
            os.makedirs(osp.join(self.ldb_dir, subdir))

    def write_data_object_meta(self) -> None:
        for id, value in self.data_object_meta_list:
            path = osp.join(self.data_object_dir, *self.oid_parts(id), "meta")
            write_data_file(path, json.dumps(value, sort_keys=True).encode(), True)

    def get_data_object_meta(self, id: str) -> Optional[DataObjectMetaRecord]:
        path = osp.join(self.data_object_dir, *self.oid_parts(id), "meta")
        try:
            return id, load_data_file(path)
        except FileNotFoundError:
            return None

    def get_data_object_meta_many(self, ids: Iterable[str]) -> Iterable[DataObjectMetaRecord]:
        for id in ids:
            record = self.get_data_object_meta(id)
            if record is not None:
                yield record

    def write_annotation(self) -> None:
        for obj in self.annotation_map.values():
            dir_path = osp.join(self.annotation_dir, *self.oid_parts(obj.oid))
            value_path = osp.join(dir_path, "user")
            meta_path = osp.join(dir_path, "ldb")
            write_data_file(value_path, obj.value_bytes, False)
            write_data_file(meta_path, obj.meta_bytes, False)

    def get_annotation(self, id: str) -> Optional[AnnotationRecord]:
        path = osp.join(self.annotation_dir, *self.oid_parts(id), "user")
        try:
            return id, load_data_file(path)
        except FileNotFoundError:
            return None

    def get_annotation_many(self, ids: Iterable[str]) -> Iterable[AnnotationRecord]:
        for id in ids:
            record = self.get_annotation(id)
            if record is not None:
                yield record

    def write_data_object_annotation(self) -> None:
        for data_object_id, annotation_id, value in self.data_object_annotation_list:
            path = osp.join(
                self.data_object_dir,
                *self.oid_parts(data_object_id),
                "annotations",
                annotation_id,
            )
            write_data_file(path, json.dumps(value, sort_keys=True).encode(), True)

    def get_pair_meta(self, id: str, annot_id: str) -> Optional[DataObjectAnnotationRecord]:
        path = osp.join(
            self.data_object_dir,
            *self.oid_parts(id),
            "annotations",
            annot_id,
        )
        try:
            return id, annot_id, load_data_file(path)
        except FileNotFoundError:
            return None

    def get_pair_meta_many(
        self,
        collection: Iterable[Tuple[str, Optional[str]]],
    ) -> Iterable[DataObjectAnnotationRecord]:
        for data_object_id, annotation_id in collection:
            record = self.get_pair_meta(data_object_id, annotation_id or "")
            if record is not None:
                yield record

    def get_pair_meta_all(self) -> Iterable[DataObjectAnnotationRecord]:
        raise NotImplementedError

    def count_pairs(self, id: str) -> int:
        path = osp.join(
            self.data_object_dir,
            *self.oid_parts(id),
            "annotations",
        )
        try:
            names = os.listdir(path)
        except FileNotFoundError:
            return 0
        return len(names)

    def ls_collection(
        self, collection: Iterable[Tuple[str, Optional[str]]]
    ) -> Iterable[Tuple[str, str, str, int]]:
        for data_object_id, annotation_id in collection:
            annotation_version: int = 0
            if annotation_id:
                pair_record = self.get_pair_meta(data_object_id, annotation_id)
                if pair_record is None:
                    raise ValueError(
                        "No record found for data object / annotation pair: "
                        f"{data_object_id=} {annotation_id=}"
                    )
                annotation_meta = pair_record[2]
                annotation_version = cast(int, annotation_meta["version"])

            record = self.get_data_object_meta(data_object_id)
            if record is None:
                raise DataObjectNotFoundError(
                    f"Data object not found: {DATA_OBJ_ID_PREFIX}{data_object_id}"
                )
            data_object_path: str = record[1]["fs"]["path"]  # type: ignore[assignment,index]
            yield data_object_id, data_object_path, annotation_id or "", annotation_version

    def set_current_annot(self, id: str, annot_id: str) -> None:
        path = osp.join(self.data_object_dir, *self.oid_parts(id), "current")
        write_data_file(path, annot_id.encode(), True)

    def get_root_collection(self) -> Iterable[Tuple[str, str]]:
        from ldb.dataset import get_collection_dir_items

        for data_object_id, annotation_id in get_collection_dir_items(
            Path(osp.join(self.ldb_dir, InstanceDir.DATA_OBJECT_INFO)),
            is_workspace=False,
        ):
            yield data_object_id, annotation_id or ""

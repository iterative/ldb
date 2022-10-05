import json
import os
import os.path as osp
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Iterable, Optional, Tuple

from ldb.db.abstract import (
    AbstractDB,
    AnnotationRecord,
    DataObjectAnnotationRecord,
    DataObjectMetaRecord,
)
from ldb.objects.annotation import Annotation
from ldb.path import INSTANCE_DIRS, InstanceDir
from ldb.typing import JSONDecoded
from ldb.utils import json_dumps, load_data_file, write_data_file

if TYPE_CHECKING:
    from ldb.index.utils import AnnotationMeta
    from ldb.index.utils import DataObjectMeta as DataObjectMetaT


class FileDB(AbstractDB):
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.ldb_dir = path
        self.annotation_dir = osp.join(self.ldb_dir, InstanceDir.ANNOTATIONS)
        self.data_object_dir = osp.join(self.ldb_dir, InstanceDir.DATA_OBJECT_INFO)

    @staticmethod
    def oid_parts(id: str) -> Tuple[str, str]:
        return id[:3], id[3:]

    def init(self):
        for subdir in INSTANCE_DIRS:
            os.makedirs(osp.join(self.ldb_dir, subdir))

    # def add_data_object_meta(self, id: str, value: "DataObjectMetaT") -> None:
    #    super().add_data_object_meta(id, value)
    #    self.write_data_object_meta()

    def write_data_object_meta(self) -> None:
        for id, value in self.data_object_meta_list:
            path = osp.join(self.data_object_dir, *self.oid_parts(id), "meta")
            write_data_file(path, json.dumps(value).encode(), True)

    def get_data_object_meta(self, id: str) -> DataObjectMetaRecord:
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
        for obj in self.annotation_list:
            dir_path = osp.join(self.annotation_dir, *self.oid_parts(obj.oid))
            value_path = osp.join(dir_path, "user")
            meta_path = osp.join(dir_path, "ldb")
            write_data_file(value_path, obj.value_bytes, False)
            write_data_file(meta_path, obj.meta_bytes, False)

    def get_annotation(self, id: str) -> AnnotationRecord:
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
            write_data_file(path, json.dumps(value).encode(), True)

    def get_pair_meta(self, id: str, annot_id: str) -> DataObjectAnnotationRecord:
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

    def ls_collection(self, collection: Iterable[Tuple[str, Optional[str]]]):
        for data_object_hash, annotation_hash in collection:
            data_object_dir = osp.join(
                self.data_object_dir,
                *self.oid_parts(data_object_hash),
            )
            annotation_version = 0
            if annotation_hash:
                annotation_meta = load_data_file(
                    osp.join(data_object_dir, "annotations", annotation_hash),
                )
                annotation_version = annotation_meta["version"]
            data_object_path = load_data_file(osp.join(data_object_dir, "meta"),)[
                "fs"
            ]["path"]

            yield data_object_hash, data_object_path, annotation_hash, annotation_version

    def set_current_annot(self, id: str, annot_id: str):
        path = osp.join(self.data_object_dir, *self.oid_parts(id), "current")
        write_data_file(path, annot_id.encode(), True)

    def get_root_collection(self):
        from ldb.dataset import get_collection_dir_items

        return dict(
            get_collection_dir_items(
                Path(osp.join(self.ldb_dir, InstanceDir.DATA_OBJECT_INFO)),
                is_workspace=False,
            ),
        )

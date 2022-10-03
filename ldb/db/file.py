import os
import os.path as osp
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Optional, Tuple, Union

from ldb.db.abstract import AbstractDB
from ldb.objects.annotation import Annotation
from ldb.path import INSTANCE_DIRS, InstanceDir
from ldb.utils import (
    get_hash_path,
    json_dumps,
    load_data_file,
    write_data_file,
)

if TYPE_CHECKING:
    from ldb.index.utils import AnnotationMeta
    from ldb.index.utils import DataObjectMeta as DataObjectMetaT


class FileDB(AbstractDB):
    def __init__(self, ldb_dir: Union[str, Path]):
        self.ldb_dir = str(ldb_dir)
        self.annotation_dir = osp.join(ldb_dir, InstanceDir.ANNOTATIONS)
        self.data_object_dir = osp.join(ldb_dir, InstanceDir.DATA_OBJECT_INFO)

    @staticmethod
    def oid_parts(id: str) -> Tuple[str, str]:
        return id[:3], id[3:]

    def init(self):
        for subdir in INSTANCE_DIRS:
            os.makedirs(osp.join(self.ldb_dir, subdir))

    def write_all(self):
        pass

    def add_pair(
        self,
        data_object_hash: str,
        data_object_meta: "DataObjectMetaT",
        annotation: Optional[Annotation] = None,
        annotation_meta: Optional["AnnotationMeta"] = None,
    ) -> None:
        self.add_data_object_meta(data_object_hash, data_object_meta)
        if annotation is not None:
            self.add_annotation(annotation)
            self.add_pair_meta(
                data_object_hash,
                annotation.oid,
                annotation_meta,
            )
            self.set_current_annot(data_object_hash, annotation.oid)

    def add_data_object_meta(self, id, obj):
        path = osp.join(self.data_object_dir, *self.oid_parts(id), "meta")
        write_data_file(path, json_dumps(obj).encode(), True)

    def get_data_object_meta(self, id):
        path = osp.join(self.data_object_dir, *self.oid_parts(id), "meta")
        try:
            return load_data_file(path)
        except FileNotFoundError:
            return None

    def get_data_object_meta_many(self, ids):
        return {
            id: value
            for id in ids
            if (value := self.get_data_object_meta(id)) is not None
        }

    def add_annotation(self, obj: Annotation):
        dir_path = osp.join(self.annotation_dir, *self.oid_parts(obj.oid))
        value_path = osp.join(dir_path, "user")
        meta_path = osp.join(dir_path, "ldb")
        write_data_file(value_path, obj.value_bytes, False)
        write_data_file(meta_path, obj.meta_bytes, False)

    def get_annotation(self, id: str):
        path = osp.join(self.annotation_dir, *self.oid_parts(id), "user")
        try:
            return load_data_file(path)
        except FileNotFoundError:
            return None

    def get_annotation_many(self, ids):
        for id in ids:
            yield id, self.get_annotation(id)

    def add_pair_meta(self, id, annot_id, obj):
        path = osp.join(
            self.data_object_dir,
            *self.oid_parts(id),
            "annotations",
            annot_id,
        )
        write_data_file(path, json_dumps(obj).encode(), True)

    def get_pair_meta(self, id: str, annot_id: str):
        path = osp.join(
            self.data_object_dir,
            *self.oid_parts(id),
            "annotations",
            annot_id,
        )
        try:
            return load_data_file(path)
        except FileNotFoundError:
            return None

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
                self.data_object_dir, *self.oid_parts(data_object_hash),
            )
            annotation_version = 0
            if annotation_hash:
                annotation_meta = load_data_file(
                    osp.join(data_object_dir, "annotations", annotation_hash),
                )
                annotation_version = annotation_meta["version"]
            data_object_path = load_data_file(
                osp.join(data_object_dir, "meta"),
            )["fs"]["path"]

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

import json
import os
import os.path as osp
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    cast,
)

from ldb.dataset import Dataset
from ldb.db.abstract import (
    AbstractDB,
    AnnotationRecord,
    DataObjectAnnotationRecord,
    DataObjectMetaRecord,
)
from ldb.exceptions import (
    CollectionNotFoundError,
    DataObjectNotFoundError,
    DatasetNotFoundError,
    DatasetVersionNotFoundError,
    RecordNotFoundError,
)
from ldb.objects.dataset_version import DatasetVersion
from ldb.path import FILE_DB_DIRS, InstanceDir
from ldb.transform import Transform
from ldb.utils import DATA_OBJ_ID_PREFIX, load_data_file, write_data_file

if TYPE_CHECKING:
    from ldb.index.utils import AnnotationMeta  # noqa: F401
    from ldb.index.utils import DataObjectMeta as DataObjectMetaT  # noqa: F401


class FileDB(AbstractDB):
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.ldb_dir = path
        self.data_object_dir = osp.join(self.ldb_dir, InstanceDir.DATA_OBJECT_INFO)
        self.annotation_dir = osp.join(self.ldb_dir, InstanceDir.ANNOTATIONS)
        self.collection_dir = osp.join(self.ldb_dir, InstanceDir.COLLECTIONS)
        self.dataset_version_dir = osp.join(self.ldb_dir, InstanceDir.DATASET_VERSIONS)
        self.dataset_dir = osp.join(self.ldb_dir, InstanceDir.DATASETS)
        self.transform_dir = osp.join(self.ldb_dir, InstanceDir.TRANSFORMS)
        self.transform_mapping_dir = osp.join(self.ldb_dir, InstanceDir.TRANSFORM_MAPPINGS)

    @staticmethod
    def oid_parts(id: str) -> Tuple[str, str]:
        return id[:3], id[3:]

    def init(self) -> None:
        for subdir in FILE_DB_DIRS:
            os.makedirs(osp.join(self.ldb_dir, subdir))

    def write_data_object_meta(self) -> None:
        for id, value in self.data_object_meta_list:
            path = osp.join(self.data_object_dir, *self.oid_parts(id), "meta")
            write_data_file(path, json.dumps(value, sort_keys=True).encode(), True)
        self.data_object_meta_list = []

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

    def get_data_object_meta_all(self) -> Iterable[DataObjectMetaRecord]:
        raise NotImplementedError

    def get_existing_data_object_ids(self, ids: Iterable[str]) -> Set[str]:
        base = self.data_object_dir
        result = set()
        for id in ids:
            if id not in result and osp.exists(osp.join(base, *self.oid_parts(id))):
                result.add(id)
        return result

    def write_annotation(self) -> None:
        for obj in self.annotation_map.values():
            dir_path = osp.join(self.annotation_dir, *self.oid_parts(obj.oid))
            value_path = osp.join(dir_path, "user")
            meta_path = osp.join(dir_path, "ldb")
            write_data_file(value_path, obj.value_bytes, False)
            write_data_file(meta_path, obj.meta_bytes, False)
        self.annotation_map = {}

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

    def get_annotation_all(self) -> Iterable[AnnotationRecord]:
        base = self.annotation_dir
        for part1 in os.listdir(base):
            path1 = osp.join(base, part1)
            for part2 in os.listdir(path1):
                id = f"{part1}{part2}"
                path = osp.join(path1, part2, "user")
                yield id, load_data_file(path)

    def write_data_object_annotation(self) -> None:
        for data_object_id, annotation_id, value in self.data_object_annotation_list:
            path = osp.join(
                self.data_object_dir,
                *self.oid_parts(data_object_id),
                "annotations",
                annotation_id,
            )
            write_data_file(path, json.dumps(value, sort_keys=True).encode(), True)
        self.data_object_annotation_list = []

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
        base = self.data_object_dir
        for part1 in os.listdir(base):
            path1 = osp.join(base, part1)
            for part2 in os.listdir(path1):
                data_object_id = f"{part1}{part2}"
                annotation_dir = osp.join(path1, part2, "annotations")
                for annotation_id in os.listdir(annotation_dir):
                    path = osp.join(annotation_dir, annotation_id)
                    yield data_object_id, annotation_id, load_data_file(path)

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

    def write_collection(self) -> None:
        if self.collection_map:
            for id, collection_obj in self.collection_map.items():
                write_data_file(
                    osp.join(self.collection_dir, *self.oid_parts(id)),
                    collection_obj.bytes,
                    overwrite_existing=False,
                )
            self.collection_map = {}

    def get_collection(self, id: str) -> Iterable[Tuple[str, str]]:
        try:
            collection = load_data_file(osp.join(self.collection_dir, *self.oid_parts(id)))
        except FileNotFoundError:
            raise CollectionNotFoundError(f"Collection not found: {id}")
        yield from collection.items()

    def get_collection_id_all(self) -> Iterable[str]:
        base = self.collection_dir
        for part1 in os.listdir(base):
            path1 = osp.join(base, part1)
            for part2 in os.listdir(path1):
                yield f"{part1}{part2}"

    def get_root_collection(self) -> Iterable[Tuple[str, str]]:
        from ldb.dataset import get_collection_dir_items

        for data_object_id, annotation_id in get_collection_dir_items(
            Path(osp.join(self.ldb_dir, InstanceDir.DATA_OBJECT_INFO)),
            is_workspace=False,
        ):
            yield data_object_id, annotation_id or ""

    def set_current_annot(self, id: str, annot_id: str) -> None:
        if annot_id:
            path = osp.join(self.data_object_dir, *self.oid_parts(id), "current")
            write_data_file(path, annot_id.encode(), True)

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

    def write_dataset_version(self) -> None:
        base = self.dataset_version_dir
        for id, dataset_version in self.dataset_version_map.items():
            path = osp.join(base, *self.oid_parts(id))
            write_data_file(path, dataset_version.bytes)
        self.dataset_version_map = {}

    def get_dataset_version(self, id: str) -> "DatasetVersion":
        path = osp.join(self.dataset_version_dir, *self.oid_parts(id))
        obj = DatasetVersion.parse(load_data_file(path))
        obj.oid = id
        return obj

    def get_dataset_version_many(self, ids: Iterable[str]) -> Iterator["DatasetVersion"]:
        for id in ids:
            yield self.get_dataset_version(id)

    def get_dataset_version_id_all(self) -> Iterable[str]:
        base = self.dataset_version_dir
        for part1 in os.listdir(base):
            path1 = osp.join(base, part1)
            for part2 in os.listdir(path1):
                yield f"{part1}{part2}"

    def get_dataset_version_by_name(
        self, name: str, version: Optional[int] = None
    ) -> Tuple["DatasetVersion", int]:
        try:
            dataset_obj = self.get_dataset(name)
        except FileNotFoundError as exc:
            raise DatasetNotFoundError(name) from exc
        if not version:
            dataset_version_hash = dataset_obj.versions[-1]
            version = len(dataset_obj.versions)
        else:
            try:
                dataset_version_hash = dataset_obj.versions[version - 1]
            except IndexError as exc:
                latest_version = len(dataset_obj.versions)
                raise DatasetVersionNotFoundError(
                    f"Dataset {name} does not have version {version}\n"
                    f"The latest version is {latest_version}"
                ) from exc
        return self.get_dataset_version(dataset_version_hash), version

    def write_dataset_assignment(self) -> None:
        base = self.dataset_dir
        for name, dataset_versions in self.dataset_version_assignments.items():
            path = osp.join(base, name)
            try:
                dataset = Dataset.parse(load_data_file(path))
            except FileNotFoundError:
                dataset = Dataset(
                    name=name,
                    created_by=dataset_versions[0].commit_info.created_by,
                    created=dataset_versions[0].commit_info.commit_time,
                    versions=[],
                )
            id_set = set(dataset.versions)
            for version in dataset_versions:
                if version.oid not in id_set:
                    id_set.add(version.oid)
                    dataset.versions.append(version.oid)
            write_data_file(
                path,
                json.dumps(dataset.format()).encode(),
                overwrite_existing=True,
            )
        self.dataset_version_assignments = {}

    def get_dataset(self, name: str) -> "Dataset":
        try:
            return Dataset.parse(load_data_file(osp.join(self.dataset_dir, name)))
        except FileNotFoundError:
            raise DatasetNotFoundError(f"Dataset not found: {name}")

    def get_dataset_many(self, names: Iterable[str]) -> Iterable["Dataset"]:
        for name in names:
            yield self.get_dataset(name)

    def get_dataset_all(self) -> Iterable["Dataset"]:
        for name in os.listdir(self.dataset_dir):
            yield self.get_dataset(name)

    def write_transform(self) -> None:
        base = self.transform_dir
        for transform in self.transforms:
            path = osp.join(base, *self.oid_parts(transform.obj_id))
            write_data_file(path, transform.json.encode(), overwrite_existing=False)
        self.transforms = set()

    def get_transform(self, id: str) -> "Transform":
        path = osp.join(self.transform_dir, *self.oid_parts(id))
        return Transform.from_generic(
            **load_data_file(path),
        )

    def get_transform_many(self, ids: Iterable[str]) -> Iterable["Transform"]:
        for id in ids:
            yield self.get_transform(id)

    def get_transform_all(self) -> Iterable["Transform"]:
        base = self.transform_dir
        for part1 in os.listdir(base):
            path1 = osp.join(base, part1)
            for part2 in os.listdir(path1):
                path = osp.join(path1, part2)
                yield Transform.from_generic(**load_data_file(path))

    def write_transform_mapping(self) -> None:
        base = self.transform_mapping_dir
        for id, transform_mapping in self.transform_mappings.items():
            path = osp.join(base, *self.oid_parts(id))
            write_data_file(
                path,
                transform_mapping.bytes,
                overwrite_existing=False,
            )
        self.transform_mappings = {}

    def get_transform_mapping(self, id: str) -> Iterable[Tuple[str, List[str]]]:
        try:
            mapping = load_data_file(osp.join(self.transform_mapping_dir, *self.oid_parts(id)))
        except FileNotFoundError:
            raise RecordNotFoundError(f"Transform mapping not found: {id}")
        yield from mapping.items()

    def get_transform_mapping_id_all(self) -> Iterable[str]:
        base = self.transform_mapping_dir
        for part1 in os.listdir(base):
            path1 = osp.join(base, part1)
            for part2 in os.listdir(path1):
                yield f"{part1}{part2}"

    def check_for_missing_data_object_ids(self, ids: Iterable[str]) -> None:
        base = self.data_object_dir
        for id in ids:
            if not osp.exists(osp.join(base, *self.oid_parts(id))):
                raise DataObjectNotFoundError(
                    f"Data object not found: {DATA_OBJ_ID_PREFIX}{id}"
                )

    def get_current_annotation_hashes(self, data_object_ids: Iterable[str]) -> Iterable[str]:
        base = self.data_object_dir
        for id in data_object_ids:
            data_object_dir = osp.join(base, *self.oid_parts(id))
            if not osp.isdir(data_object_dir):
                raise DataObjectNotFoundError(
                    f"Data object not found: {DATA_OBJ_ID_PREFIX}{id}"
                )
            try:
                with open(osp.join(data_object_dir, "current")) as f:
                    annot_id = f.read()
                yield annot_id
            except FileNotFoundError:
                yield ""

    def get_annotation_version_hashes(
        self, data_object_ids: Iterable[str], version: int = -1
    ) -> Iterable[Tuple[str, str]]:
        base = self.data_object_dir
        for id in data_object_ids:
            path = osp.join(base, *self.oid_parts(id), "annotations")
            result = ""
            if osp.isdir(path):
                annot_ids = os.listdir(path)
                expected_version = len(annot_ids) if version == -1 else version
                for annot_id in annot_ids:
                    annot = load_data_file(osp.join(path, annot_id))
                    if annot["version"] == expected_version:
                        result = annot_id
            yield id, result

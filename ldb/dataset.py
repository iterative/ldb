import json
import os
import random
from abc import ABC
from collections import defaultdict
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from glob import iglob
from itertools import tee
from pathlib import Path
from typing import (
    Any,
    Callable,
    DefaultDict,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from funcy.objects import cached_property

from ldb.collections import LDBMappingCache
from ldb.exceptions import DatasetNotFoundError, LDBException
from ldb.iter_utils import take
from ldb.op_type import OpType
from ldb.path import InstanceDir
from ldb.query.search import BoolSearchFunc, get_bool_search_func
from ldb.typing import JSONDecoded, JSONObject
from ldb.utils import (
    format_dataset_identifier,
    format_datetime,
    get_hash_path,
    load_data_file,
    parse_datetime,
)

OpDef = Tuple[str, Union[str, int, float, List[str]]]
CollectionFunc = Callable[
    [Iterable[Tuple[str, str]]],
    Iterator[Tuple[str, str]],
]


@dataclass
class CommitInfo:
    created_by: str
    commit_time: datetime
    commit_message: str

    @classmethod
    def parse(cls, attr_dict: Dict[str, str]) -> "CommitInfo":
        attr_dict = attr_dict.copy()
        return cls(
            commit_time=parse_datetime(attr_dict.pop("commit_time")),
            **attr_dict,
        )

    def format(self) -> Dict[str, str]:
        attr_dict = asdict(self)
        return dict(
            commit_time=format_datetime(attr_dict.pop("commit_time")),
            **attr_dict,
        )


@dataclass
class DatasetVersion:
    version: int
    parent: str
    collection: str
    tags: List[str]
    commit_info: CommitInfo

    @classmethod
    def parse(cls, attr_dict: Dict[str, Any]) -> "DatasetVersion":
        attr_dict = attr_dict.copy()
        return cls(
            commit_info=CommitInfo.parse(attr_dict.pop("commit_info")),
            **attr_dict,
        )

    def format(self) -> Dict[str, Any]:
        attr_dict = {f.name: getattr(self, f.name) for f in fields(self)}
        return dict(
            commit_info=attr_dict.pop("commit_info").format(),
            tags=attr_dict.pop("tags").copy(),
            **attr_dict,
        )


@dataclass
class Dataset:
    name: str
    created_by: str
    created: datetime
    versions: List[str]

    @classmethod
    def parse(cls, attr_dict: Dict[str, Any]) -> "Dataset":
        attr_dict = attr_dict.copy()
        created = parse_datetime(attr_dict.pop("created"))
        return cls(created=created, **attr_dict)

    def format(self) -> Dict[str, Any]:
        attr_dict = asdict(self)
        created = format_datetime(attr_dict.pop("created"))
        return dict(created=created, **attr_dict)


def iter_collection_dir(collection_dir: Union[str, Path]) -> Iterator[str]:
    return iglob(os.path.join(collection_dir, "*", "*"))


def get_collection(
    ldb_dir: Path,
    dataset_version_hash: str,
) -> Dict[str, Optional[str]]:
    dataset_version_obj = DatasetVersion.parse(
        load_data_file(
            get_hash_path(
                ldb_dir / InstanceDir.DATASET_VERSIONS,
                dataset_version_hash,
            ),
        ),
    )
    return load_data_file(  # type: ignore[no-any-return]
        get_hash_path(
            ldb_dir / InstanceDir.COLLECTIONS,
            dataset_version_obj.collection,
        ),
    )


def get_collection_from_dataset_identifier(
    ldb_dir: Path,
    dataset_name: str,
    dataset_version: Optional[int] = None,
) -> Dict[str, Optional[str]]:
    dataset = get_dataset(ldb_dir, dataset_name)
    dataset_version_hash = get_dataset_version_hash(dataset, dataset_version)
    return get_collection(ldb_dir, dataset_version_hash)


def get_collection_dir_keys(
    collection_dir: Union[str, Path],
) -> Iterator[str]:
    for path in iter_collection_dir(collection_dir):
        parent, name = os.path.split(path)
        yield os.path.basename(parent) + name


def get_collection_dir_items(
    collection_dir: Path,
    is_workspace: bool = True,
) -> Iterator[Tuple[str, Optional[str]]]:
    annotation_hash_func = (
        get_workspace_collection_annotation_hash
        if is_workspace
        else get_root_collection_annotation_hash
    )
    for path in sorted(collection_dir.glob("*/*")):
        yield path.parent.name + path.name, annotation_hash_func(path)


def get_collection_size(
    collection_dir: Union[str, Path],
) -> int:
    return sum(1 for _ in iter_collection_dir(collection_dir))


def get_root_collection_annotation_hash(
    data_object_path: Path,
) -> Optional[str]:
    try:
        return (data_object_path / "current").read_text()
    except FileNotFoundError:
        return None


def get_workspace_collection_annotation_hash(
    data_object_path: Path,
) -> Optional[str]:
    return data_object_path.read_text() or None


def combine_collections(
    ldb_dir: Path,
    collections: List[Dict[str, Optional[str]]],
) -> Dict[str, str]:
    all_versions: DefaultDict[str, List[str]] = defaultdict(list)
    for collection in collections:
        for data_object_hash, annotation_hash in collection.items():
            lst = all_versions[data_object_hash]
            if annotation_hash:
                lst.append(annotation_hash)
    combined_collection = {}
    for data_object_hash, annotation_hashes in sorted(all_versions.items()):
        if len(annotation_hashes) > 1:
            annotation_dir = (
                get_hash_path(
                    ldb_dir / InstanceDir.DATA_OBJECT_INFO,
                    data_object_hash,
                )
                / "annotations"
            )
            latest_annotation_hash = max(
                (load_data_file(annotation_dir / h)["version"], h)
                for h in annotation_hashes
            )[1]
        elif annotation_hashes:
            latest_annotation_hash = annotation_hashes[0]
        else:
            latest_annotation_hash = ""
        combined_collection[data_object_hash] = latest_annotation_hash
    return combined_collection


def get_dataset(ldb_dir: Path, dataset_name: str) -> Dataset:
    try:
        return Dataset.parse(
            load_data_file(ldb_dir / InstanceDir.DATASETS / dataset_name),
        )
    except FileNotFoundError as exc:
        raise DatasetNotFoundError(
            f"Dataset not found with name {dataset_name!r}",
        ) from exc


def iter_dataset_dir(
    ldb_dir: Union[str, Path],
) -> Iterator[os.DirEntry]:  # type: ignore[type-arg]
    yield from os.scandir(os.path.join(ldb_dir, InstanceDir.DATASETS))


def iter_datasets(ldb_dir: Union[str, Path]) -> Iterator[Dataset]:
    for entry in iter_dataset_dir(ldb_dir):
        yield Dataset.parse(load_data_file(Path(entry.path)))


def get_dataset_version_hash(
    dataset: Dataset,
    dataset_version: Optional[int] = None,
) -> str:
    if not dataset_version:
        dataset_version = len(dataset.versions)
    try:
        return dataset.versions[dataset_version - 1]
    except IndexError as exc:
        dataset_identifier = format_dataset_identifier(
            dataset.name,
            dataset_version,
        )
        latest_dataset_identifier = format_dataset_identifier(
            dataset.name,
            len(dataset.versions),
        )
        raise LDBException(
            f"{dataset_identifier} does not exist\n"
            f"The latest version is {latest_dataset_identifier}",
        ) from exc


def get_annotation(ldb_dir: Path, annotation_hash: str) -> JSONDecoded:
    if not annotation_hash:
        return None
    user_annotation_file_path = (
        get_hash_path(
            ldb_dir / InstanceDir.ANNOTATIONS,
            annotation_hash,
        )
        / "user"
    )
    with open(user_annotation_file_path, encoding="utf-8") as f:
        data = f.read()
    return json.loads(data)  # type: ignore[no-any-return]


def get_annotations(
    ldb_dir: Path,
    annotation_hashes: Iterable[str],
) -> List[JSONDecoded]:
    annotations = []
    for annotation_hash in annotation_hashes:
        if annotation_hash:
            annotations.append(get_annotation(ldb_dir, annotation_hash))
        else:
            annotations.append(None)
    return annotations


def get_data_object_meta(
    ldb_dir: Path,
    data_object_hash: str,
) -> JSONObject:
    meta_file_path = (
        get_hash_path(
            ldb_dir / InstanceDir.DATA_OBJECT_INFO,
            data_object_hash,
        )
        / "meta"
    )
    meta: JSONObject = json.loads(meta_file_path.read_text())
    return meta


def get_data_object_metas(
    ldb_dir: Path,
    data_object_hashes: Iterable[str],
) -> List[JSONObject]:
    meta_objects = []
    for data_object_hash in data_object_hashes:
        meta_objects.append(get_data_object_meta(ldb_dir, data_object_hash))
    return meta_objects


class AnnotationCache(LDBMappingCache[str, JSONDecoded]):
    def get_new(self, key: str) -> JSONDecoded:
        return get_annotation(self.ldb_dir, key)


class DataObjectMetaCache(LDBMappingCache[str, JSONDecoded]):
    def get_new(self, key: str) -> JSONDecoded:
        return get_data_object_meta(self.ldb_dir, key)


class CollectionOperation(ABC):
    def apply(
        self,
        collection: Iterable[Tuple[str, str]],
    ) -> Iterator[Tuple[str, str]]:
        raise NotImplementedError


class Limit(CollectionOperation):
    def __init__(self, n: int) -> None:
        self.n = n

    def apply(
        self,
        collection: Iterable[Tuple[str, str]],
    ) -> Iterator[Tuple[str, str]]:
        return take(collection, n=self.n)


class Sample(CollectionOperation):
    def __init__(self, p: float) -> None:
        self.p = p

    def apply(
        self,
        collection: Iterable[Tuple[str, str]],
    ) -> Iterator[Tuple[str, str]]:
        for c in collection:
            if random.random() <= self.p:
                yield c


class Sort(CollectionOperation):
    def __init__(self, ldb_dir: Path, proc_args: List[str]) -> None:
        self.ldb_dir = ldb_dir
        self.proc_args = proc_args

    def apply(
        self,
        collection: Iterable[Tuple[str, str]],
    ) -> Iterator[Tuple[str, str]]:
        from ldb.pipe import (  # pylint: disable=import-outside-toplevel
            sort_collection,
        )

        return sort_collection(self.ldb_dir, collection, self.proc_args)


class Query(CollectionOperation):
    def __init__(
        self,
        ldb_dir: Path,
        cache: LDBMappingCache[Any, Any],
        search: BoolSearchFunc,
    ) -> None:
        self.ldb_dir = ldb_dir
        self.cache = cache
        self.search = search


class AnnotationQuery(Query):
    def apply(
        self,
        collection: Iterable[Tuple[str, str]],
    ) -> Iterator[Tuple[str, str]]:
        collection1, collection2 = tee(collection)
        for (data_object_hash, annotation_hash), keep in zip(
            collection1,
            self.search(self.cache[a] for _, a in collection2),
        ):
            if keep:
                yield data_object_hash, annotation_hash


class FileQuery(Query):
    def apply(
        self,
        collection: Iterable[Tuple[str, str]],
    ) -> Iterator[Tuple[str, str]]:
        collection1, collection2 = tee(collection)
        for (data_object_hash, annotation_hash), keep in zip(
            collection1,
            self.search(self.cache[d] for d, _ in collection2),
        ):
            if keep:
                yield data_object_hash, annotation_hash


class PipelineData:
    def __init__(self, ldb_dir: Path) -> None:
        self.ldb_dir = ldb_dir

    @cached_property
    def data_object_metas(self) -> DataObjectMetaCache:
        return DataObjectMetaCache(self.ldb_dir)

    @cached_property
    def annotations(self) -> AnnotationCache:
        return AnnotationCache(self.ldb_dir)


class PipelineBuilder:
    def __init__(
        self,
        ldb_dir: Path,
        data: Optional[PipelineData] = None,
    ) -> None:
        self.ldb_dir = ldb_dir
        if data is None:
            self.data = PipelineData(ldb_dir)
        else:
            self.data = data

    def build(
        self,
        op_defs: Iterable[OpDef],
    ) -> List[CollectionFunc]:
        ops = []
        for op_type, arg in op_defs:
            op: CollectionFunc
            if op_type == OpType.ANNOTATION_QUERY:
                assert isinstance(arg, str)
                op = self.annotation_query(arg)
            elif op_type == OpType.FILE_QUERY:
                assert isinstance(arg, str)
                op = self.file_query(arg)
            elif op_type == OpType.LIMIT:
                assert isinstance(arg, int)
                op = Limit(arg).apply
            elif op_type == OpType.SAMPLE:
                assert isinstance(arg, float)
                op = Sample(arg).apply
            elif op_type == OpType.PIPE:
                assert isinstance(arg, list)
                op = Sort(self.ldb_dir, arg).apply
            else:
                raise ValueError(f"Unknown op type: {op_type}")
            ops.append(op)
        return ops

    def annotation_query(self, search: str) -> CollectionFunc:
        return AnnotationQuery(
            self.ldb_dir,
            self.data.annotations,
            get_bool_search_func(search),
        ).apply

    def file_query(self, search: str) -> CollectionFunc:
        return FileQuery(
            self.ldb_dir,
            self.data.data_object_metas,
            get_bool_search_func(search),
        ).apply


class Pipeline:
    def __init__(self, ops: Iterable[CollectionFunc]) -> None:
        self.ops = ops

    @classmethod
    def from_defs(
        cls,
        ldb_dir: Path,
        op_defs: Iterable[OpDef],
        data: Optional[PipelineData] = None,
    ) -> "Pipeline":
        return cls(PipelineBuilder(ldb_dir, data=data).build(op_defs))

    def run(
        self,
        collection: Iterable[Tuple[str, str]],
    ) -> Iterator[Tuple[str, str]]:
        for op in self.ops:
            collection = op(collection)
        return iter(collection)


def apply_queries(
    ldb_dir: Path,
    data_object_hashes: Iterable[str],
    annotation_hashes: Iterable[str],
    op_defs: Iterable[OpDef],
) -> Iterator[Tuple[str, str]]:
    """
    Filter the given collection by the operations in `collection_ops`.
    """
    collection = zip(data_object_hashes, annotation_hashes)
    return Pipeline.from_defs(ldb_dir, op_defs).run(collection)

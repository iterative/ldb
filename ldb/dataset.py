import json
import os
from collections import defaultdict
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from glob import iglob
from pathlib import Path
from typing import (
    Any,
    DefaultDict,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from ldb.exceptions import DatasetNotFoundError, LDBException
from ldb.path import InstanceDir
from ldb.query.search import BoolSearchFunc
from ldb.utils import (
    format_dataset_identifier,
    format_datetime,
    get_hash_path,
    load_data_file,
    parse_datetime,
)


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


def get_annotations(
    ldb_dir: Path,
    annotation_hashes: Iterable[str],
) -> List[Optional[str]]:
    annotations = []
    for annotation_hash in annotation_hashes:
        if annotation_hash:
            user_annotation_file_path = (
                get_hash_path(
                    ldb_dir / InstanceDir.ANNOTATIONS,
                    annotation_hash,
                )
                / "user"
            )
            annotations.append(
                json.loads(user_annotation_file_path.read_text()),
            )
        else:
            annotations.append(None)
    return annotations


def get_data_object_meta(
    ldb_dir: Path,
    data_object_hashes: Iterable[str],
) -> List[str]:
    meta_objects = []
    for data_object_hash in data_object_hashes:
        meta_file_path = (
            get_hash_path(
                ldb_dir / InstanceDir.DATA_OBJECT_INFO,
                data_object_hash,
            )
            / "meta"
        )
        meta_objects.append(json.loads(meta_file_path.read_text()))
    return meta_objects


def apply_query(
    ldb_dir: Path,
    search: BoolSearchFunc,
    data_object_hashes: Iterable[str],
    annotation_hashes: Iterable[str],
) -> Dict[str, str]:
    """
    Return a collection where the annotations pass the given search function.

    `data_object_hashes` and `annotation_hashes` should contain
    corresponding items of a collection. Only items where the given
    search function returns `True` are kept in the resulting collection.
    """
    return {
        data_object_hash: annotation_hash
        for data_object_hash, annotation_hash, keep in zip(
            data_object_hashes,
            annotation_hashes,
            search(get_annotations(ldb_dir, annotation_hashes)),
        )
        if keep
    }


def apply_query_to_data_objects(
    ldb_dir: Path,
    search: BoolSearchFunc,
    data_object_hashes: Iterable[str],
    annotation_hashes: Iterable[str],
) -> List[str]:
    """
    Return data objects whose annotations pass the given search function.

    This is similar to calling `list(result.keys())` on the result of
    `apply_query`.
    """
    return [
        data_object_hash
        for data_object_hash, keep in zip(
            data_object_hashes,
            search(get_annotations(ldb_dir, annotation_hashes)),
        )
        if keep
    ]


def apply_file_query_to_data_objects(
    ldb_dir: Path,
    search: BoolSearchFunc,
    data_object_hashes: Iterable[str],
) -> List[str]:
    """
    Return data objects that pass the given search function.
    """
    return [
        data_object_hash
        for data_object_hash, keep in zip(
            data_object_hashes,
            search(get_data_object_meta(ldb_dir, data_object_hashes)),
        )
        if keep
    ]


def apply_file_query(
    ldb_dir: Path,
    search: BoolSearchFunc,
    collection: Dict[str, str],
) -> Dict[str, str]:
    """
    Filter `collection` by data objects that pass the given search function.
    """
    return {
        data_object_hash: annotation_hash
        for (data_object_hash, annotation_hash), keep in zip(
            collection.items(),
            search(get_data_object_meta(ldb_dir, collection.keys())),
        )
        if keep
    }


def apply_queries(
    ldb_dir: Path,
    search: Optional[BoolSearchFunc],
    file_search: Optional[BoolSearchFunc],
    data_object_hashes: Iterable[str],
    annotation_hashes: Iterable[str],
) -> Dict[str, str]:
    """
    Filter the given collection by the search functions.

    If not `None`, `search` is applied to annotations and `file_search`
    to file attributes.
    """
    if search is None:
        collection = dict(zip(data_object_hashes, annotation_hashes))
    else:
        collection = apply_query(
            ldb_dir,
            search,
            data_object_hashes,
            annotation_hashes,
        )
    if file_search is not None:
        collection = apply_file_query(
            ldb_dir,
            file_search,
            collection,
        )
    return collection

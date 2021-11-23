import os
from enum import Enum, unique
from pathlib import Path
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
)

import fsspec

from ldb import config
from ldb.config import ConfigType
from ldb.dataset import (
    combine_collections,
    get_collection_dir_items,
    get_collection_dir_keys,
    get_collection_from_dataset_identifier,
)
from ldb.exceptions import LDBException
from ldb.index import get_storage_files_for_paths, index
from ldb.path import InstanceDir, WorkspacePath
from ldb.utils import (
    DATASET_PREFIX,
    ROOT,
    format_dataset_identifier,
    get_hash_path,
    hash_file,
    parse_data_object_hash_identifier,
    parse_dataset_identifier,
)
from ldb.workspace import collection_dir_to_object, load_workspace_dataset


@unique
class ArgType(Enum):
    ROOT_DATASET = "root dataset"
    DATASET = "dataset"
    WORKSPACE_DATASET = "workspace_dataset"
    DATA_OBJECT = "data object"
    PATH = "path"


class AddInput(NamedTuple):
    data_object_hashes: Iterable[str]
    annotation_hashes: Optional[Iterable[str]]
    message: str


def get_arg_type(paths: Sequence[str]) -> ArgType:
    if any(p == f"{DATASET_PREFIX}{ROOT}" for p in paths):
        return ArgType.ROOT_DATASET
    if any(p.startswith(DATASET_PREFIX) for p in paths):
        return ArgType.DATASET
    if any(p.startswith("0x") for p in paths):
        return ArgType.DATA_OBJECT
    if any(
        f.fs.protocol == "file"
        and f.fs.isdir(f.path + "/.ldb_workspace")
        and os.path.abspath(f.path) != os.getcwd()
        for f in fsspec.open_files(paths)
    ):
        return ArgType.WORKSPACE_DATASET
    return ArgType.PATH


def process_args_for_add(
    ldb_dir: Path,
    arg_type: ArgType,
    paths: Sequence[str],
) -> AddInput:
    return ADD_FUNCTIONS[arg_type](ldb_dir, paths)


def root_dataset_for_add(
    ldb_dir: Path,
    paths: Sequence[str],  # pylint: disable=unused-argument
) -> AddInput:
    data_object_hashes = []
    annotation_hashes = []
    for data_object_hash, annotation_hash in get_collection_dir_items(
        ldb_dir / InstanceDir.DATA_OBJECT_INFO,
        is_workspace=False,
    ):
        data_object_hashes.append(data_object_hash)
        annotation_hashes.append(annotation_hash or "")
    return AddInput(data_object_hashes, annotation_hashes, "")


def dataset_for_add(ldb_dir: Path, paths: Sequence[str]) -> AddInput:
    try:
        dataset_identifiers = [parse_dataset_identifier(p) for p in paths]
    except LDBException as exc:
        raise LDBException(
            "All paths must be the same type. "
            "Found path starting with 'ds', but unable "
            "parse all paths as a dataset identifier",
        ) from exc
    collections = [
        get_collection_from_dataset_identifier(
            ldb_dir,
            ds_name,
            ds_version,
        )
        for ds_name, ds_version in dataset_identifiers
    ]
    combined_collection = combine_collections(ldb_dir, collections)
    return AddInput(
        combined_collection.keys(),
        combined_collection.values(),
        "",
    )


def workspace_dataset_for_add(ldb_dir: Path, paths: Sequence[str]) -> AddInput:
    collections = [
        collection_dir_to_object(
            Path(path) / WorkspacePath.COLLECTION,
        )
        for path in paths
    ]
    combined_collection = combine_collections(ldb_dir, collections)
    return AddInput(
        combined_collection.keys(),
        combined_collection.values(),
        "",
    )


def data_object_for_add(
    ldb_dir: Path,  # pylint: disable=unused-argument
    paths: Sequence[str],
) -> AddInput:
    try:
        return AddInput(
            [parse_data_object_hash_identifier(p) for p in paths],
            None,
            "",
        )
    except ValueError as exc:
        raise LDBException(
            "All paths must be the same type. "
            "Found path starting with '0x', but unable "
            "parse all paths as a data object identifier",
        ) from exc


def path_for_add(ldb_dir: Path, paths: Sequence[str]) -> AddInput:
    indexing_result = index(
        ldb_dir,
        paths,
        read_any_cloud_location=(
            (
                config.load_first([ConfigType.INSTANCE])  # type: ignore[union-attr,call-overload] # noqa: E501
                or {}
            )
            .get("core", {})
            .get("read_any_cloud_location", False)
        ),
    )
    return AddInput(
        indexing_result.data_object_hashes,
        None,
        indexing_result.summary(),
    )


ADD_FUNCTIONS: Dict[ArgType, Callable[[Path, Sequence[str]], AddInput]] = {
    ArgType.ROOT_DATASET: root_dataset_for_add,
    ArgType.DATASET: dataset_for_add,
    ArgType.WORKSPACE_DATASET: workspace_dataset_for_add,
    ArgType.DATA_OBJECT: data_object_for_add,
    ArgType.PATH: path_for_add,
}


def add(
    ldb_dir: Path,
    workspace_path: Path,
    data_object_hashes: Iterable[str],
    annotation_hashes: Optional[Iterable[str]],
) -> None:
    data_object_hashes = list(data_object_hashes)
    if annotation_hashes is not None:
        annotation_hashes = list(annotation_hashes)
        if not len(data_object_hashes) == len(annotation_hashes):
            raise LDBException(
                "Number of data object hashes and annotations must be the "
                "same. "
                f"{len(data_object_hashes)} != {len(annotation_hashes)}",
            )
    else:
        annotation_hashes = get_current_annotation_hashes(
            ldb_dir,
            data_object_hashes,
        )

    workspace_path = Path(os.path.normpath(workspace_path))
    ds_name = load_workspace_dataset(workspace_path).dataset_name
    collection_dir_path = workspace_path / WorkspacePath.COLLECTION
    collection_dir_path.mkdir(exist_ok=True)

    to_write = []
    for data_object_hash, annotation_hash in zip(
        data_object_hashes,
        annotation_hashes,
    ):
        to_write.append(
            (
                get_hash_path(collection_dir_path, data_object_hash),
                annotation_hash,
            ),
        )

    for collection_member_path, annotation_hash in to_write:
        collection_member_path.parent.mkdir(exist_ok=True)
        with collection_member_path.open("w") as file:
            file.write(annotation_hash)
    ds_ident = format_dataset_identifier(ds_name)
    print(f"Added {len(data_object_hashes)} data objects to {ds_ident}")


def process_args_for_delete(
    ldb_dir: Path,
    arg_type: ArgType,
    paths: Sequence[str],
) -> List[str]:
    return DELETE_FUNCTIONS[arg_type](ldb_dir, paths)


def root_dataset_for_delete(
    ldb_dir: Path,
    paths: Sequence[str],  # pylint: disable=unused-argument
) -> List[str]:
    return list(
        get_collection_dir_keys(
            ldb_dir / InstanceDir.DATA_OBJECT_INFO,
        ),
    )


def dataset_for_delete(ldb_dir: Path, paths: Sequence[str]) -> List[str]:
    try:
        dataset_identifiers = [parse_dataset_identifier(p) for p in paths]
    except LDBException as exc:
        raise LDBException(
            "All paths must be the same type. "
            "Found path starting with 'ds', but unable "
            "parse all paths as a dataset identifier",
        ) from exc
    collections = [
        get_collection_from_dataset_identifier(
            ldb_dir,
            ds_name,
            ds_version,
        )
        for ds_name, ds_version in dataset_identifiers
    ]
    data_objects: Set[str] = set()
    for collection in collections:
        data_objects.update(collection.keys())
    return list(data_objects)


def data_object_for_delete(
    ldb_dir: Path,  # pylint: disable=unused-argument
    paths: Sequence[str],
) -> List[str]:
    try:
        return [parse_data_object_hash_identifier(p) for p in paths]
    except ValueError as exc:
        raise LDBException(
            "All paths must be the same type. "
            "Found path starting with '0x', but unable "
            "parse all paths as a data object identifier",
        ) from exc


def path_for_delete(ldb_dir: Path, paths: Sequence[str]) -> List[str]:
    indexing_result = index(
        ldb_dir,
        paths,
        read_any_cloud_location=(
            (
                config.load_first([ConfigType.INSTANCE])  # type: ignore[union-attr,call-overload] # noqa: E501
                or {}
            )
            .get("core", {})
            .get("read_any_cloud_location", False)
        ),
    )
    return indexing_result.data_object_hashes


DELETE_FUNCTIONS: Dict[ArgType, Callable[[Path, Sequence[str]], List[str]]] = {
    ArgType.ROOT_DATASET: root_dataset_for_delete,
    ArgType.DATASET: dataset_for_delete,
    ArgType.DATA_OBJECT: data_object_for_delete,
    ArgType.PATH: path_for_delete,
}


def delete(
    workspace_path: Path,
    data_object_hashes: Sequence[str],
) -> None:
    workspace_path = Path(os.path.normpath(workspace_path))
    ds_name = load_workspace_dataset(workspace_path).dataset_name
    collection_dir_path = workspace_path / WorkspacePath.COLLECTION
    if not collection_dir_path.exists():
        return

    num_deleted = 0
    for data_object_hash in data_object_hashes:
        collection_member_path = get_hash_path(
            collection_dir_path,
            data_object_hash,
        )
        if collection_member_path.exists():
            collection_member_path.unlink()
            try:
                collection_member_path.parent.rmdir()
            except OSError:
                pass
            num_deleted += 1
    ds_ident = format_dataset_identifier(ds_name)
    print(f"Deleted {num_deleted} data objects from {ds_ident}")


def get_current_annotation_hash(ldb_dir: Path, data_object_hash: str) -> str:
    data_object_dir = get_hash_path(
        ldb_dir / InstanceDir.DATA_OBJECT_INFO,
        data_object_hash,
    )
    if not data_object_dir.is_dir():
        raise LDBException(f"No data object found: {data_object_hash}")
    try:
        return (data_object_dir / "current").read_text()
    except FileNotFoundError:
        return ""


def get_current_annotation_hashes(
    ldb_dir: Path,
    data_object_hashes: Iterable[str],
) -> List[str]:
    return [
        get_current_annotation_hash(ldb_dir, d) for d in data_object_hashes
    ]


def process_args_for_ls(
    ldb_dir: Path,
    arg_type: ArgType,
    paths: Sequence[str],
) -> AddInput:
    return LS_FUNCTIONS[arg_type](ldb_dir, paths)


def root_dataset_for_ls(
    ldb_dir: Path,
    paths: Sequence[str],  # pylint: disable=unused-argument
) -> AddInput:
    data_object_hashes = []
    annotation_hashes = []
    for data_object_hash, annotation_hash in get_collection_dir_items(
        ldb_dir / InstanceDir.DATA_OBJECT_INFO,
        is_workspace=False,
    ):
        data_object_hashes.append(data_object_hash)
        annotation_hashes.append(annotation_hash or "")
    return AddInput(data_object_hashes, annotation_hashes, "")


def dataset_for_ls(ldb_dir: Path, paths: Sequence[str]) -> AddInput:
    try:
        dataset_identifiers = [parse_dataset_identifier(p) for p in paths]
    except LDBException as exc:
        raise LDBException(
            "All paths must be the same type. "
            "Found path starting with 'ds', but unable "
            "parse all paths as a dataset identifier",
        ) from exc
    collections = [
        get_collection_from_dataset_identifier(
            ldb_dir,
            ds_name,
            ds_version,
        )
        for ds_name, ds_version in dataset_identifiers
    ]
    combined_collection = combine_collections(ldb_dir, collections)
    return AddInput(
        combined_collection.keys(),
        combined_collection.values(),
        "",
    )


def data_object_for_ls(
    ldb_dir: Path,  # pylint: disable=unused-argument
    paths: Sequence[str],
) -> AddInput:
    try:
        data_object_hashes = [
            parse_data_object_hash_identifier(p) for p in paths
        ]
    except ValueError as exc:
        raise LDBException(
            "All paths must be the same type. "
            "Found path starting with '0x', but unable "
            "parse all paths as a data object identifier",
        ) from exc
    return AddInput(
        data_object_hashes,
        get_current_annotation_hashes(ldb_dir, data_object_hashes),
        "",
    )


def path_for_ls(ldb_dir: Path, paths: Sequence[str]) -> AddInput:
    paths = [os.path.abspath(p) for p in paths]
    data_object_hashes = [
        hash_file(f)
        for f in get_storage_files_for_paths(
            paths,
            default_format=False,
        )
        if not f.path.endswith(".json")
    ]
    return AddInput(
        data_object_hashes,
        get_current_annotation_hashes(ldb_dir, data_object_hashes),
        "",
    )


LS_FUNCTIONS: Dict[ArgType, Callable[[Path, Sequence[str]], AddInput]] = {
    ArgType.ROOT_DATASET: root_dataset_for_add,
    ArgType.DATASET: dataset_for_add,
    ArgType.WORKSPACE_DATASET: workspace_dataset_for_add,
    ArgType.DATA_OBJECT: data_object_for_ls,
    ArgType.PATH: path_for_ls,
}

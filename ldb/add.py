import os
from enum import Enum, unique
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from ldb import config
from ldb.config import ConfigType
from ldb.dataset import (
    combine_collections,
    get_collection_dir_items,
    get_collection_from_dataset_identifier,
)
from ldb.exceptions import LDBException
from ldb.index import index
from ldb.path import InstanceDir, WorkspacePath
from ldb.utils import (
    format_dataset_identifier,
    get_hash_path,
    parse_data_object_hash_identifier,
    parse_dataset_identifier,
)
from ldb.workspace import load_workspace_dataset


@unique
class ArgType(Enum):
    ROOT_DATASET = "root dataset"
    DATASET = "dataset"
    DATA_OBJECT = "data object"
    PATH = "path"


def get_arg_type(paths: List[str]) -> ArgType:
    if not paths:
        return ArgType.ROOT_DATASET
    if any(p.startswith("ds:") for p in paths):
        return ArgType.DATASET
    if any(p.startswith("0x") for p in paths):
        return ArgType.DATA_OBJECT
    return ArgType.PATH


def process_args_for_add(
    ldb_dir: Path,
    arg_type: ArgType,
    paths: List[str],
) -> Tuple[List[str], Optional[List[str]], str]:
    return ADD_FUNCTIONS[arg_type](ldb_dir, paths)


def root_dataset_for_add(
    ldb_dir: Path,
    paths: List[str],  # pylint: disable=unused-argument
):
    data_object_hashes = []
    annotation_hashes = []
    for data_object_hash, annotation_hash in get_collection_dir_items(
        ldb_dir / InstanceDir.DATA_OBJECT_INFO,
        is_workspace=False,
    ):
        data_object_hashes.append(data_object_hash)
        annotation_hashes.append(annotation_hash or "")
    return data_object_hashes, annotation_hashes, ""


def dataset_for_add(ldb_dir: Path, paths: List[str]):
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
    return (
        list(combined_collection.keys()),
        list(combined_collection.values()),
        "",
    )


def data_object_for_add(
    ldb_dir: Path,  # pylint: disable=unused-argument
    paths: List[str],
):
    try:
        return (
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


def path_for_add(ldb_dir: Path, paths: List[str]):
    indexing_result = index(
        ldb_dir,
        paths,
        read_any_cloud_location=(
            (config.load_first([ConfigType.INSTANCE]) or {})
            .get("core", {})
            .get("read_any_cloud_location", False)
        ),
    )
    return indexing_result.data_object_hashes, None, indexing_result.summary()


ADD_FUNCTIONS = {
    ArgType.ROOT_DATASET: root_dataset_for_add,
    ArgType.DATASET: dataset_for_add,
    ArgType.DATA_OBJECT: data_object_for_add,
    ArgType.PATH: path_for_add,
}


def add(
    ldb_dir: Path,
    workspace_path: Path,
    data_object_hashes: Sequence[str],
    annotation_hashes: Sequence[str],
) -> None:
    if annotation_hashes:
        if not len(data_object_hashes) == len(annotation_hashes):
            raise LDBException(
                "Number of data object hashes and annotations must be the "
                "same. "
                f"{len(data_object_hashes)} != {len(annotation_hashes)}",
            )
    else:
        annotation_hashes = [
            get_current_annotation_hash(ldb_dir, data_object_hash)
            for data_object_hash in data_object_hashes
        ]

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

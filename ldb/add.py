import os
from pathlib import Path
from typing import List

from ldb.exceptions import LDBException
from ldb.path import InstanceDir, WorkspacePath
from ldb.utils import format_dataset_identifier, get_hash_path, load_data_file


def add(
    ldb_dir: Path,
    workspace_path: Path,
    data_object_hashes: List[str],
) -> None:
    workspace_path = Path(os.path.normpath(workspace_path))
    try:
        workspace_ds = load_data_file(workspace_path / WorkspacePath.DATASET)
    except FileNotFoundError as exc:
        raise LDBException(
            "No workspace dataset staged at "
            f"{repr(os.fspath(workspace_path))}",
        ) from exc
    ds_name = workspace_ds["dataset_name"]
    collection_dir_path = workspace_path / WorkspacePath.COLLECTION
    collection_dir_path.mkdir(exist_ok=True)

    to_write = []
    for data_object_hash in data_object_hashes:
        to_write.append(
            (
                get_hash_path(collection_dir_path, data_object_hash),
                get_current_annotation_hash(ldb_dir, data_object_hash),
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

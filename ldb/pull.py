import os
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence, Tuple

from ldb.add import process_args_for_ls
from ldb.core import LDBClient
from ldb.dataset import (
    OpDef,
    apply_queries_to_collection,
    get_collection_dir_keys,
)
from ldb.path import WorkspacePath
from ldb.typing import JSONObject
from ldb.utils import format_dataset_identifier, get_hash_path
from ldb.workspace import load_workspace_dataset


def pull(
    ldb_dir: Path,
    workspace_path: Path,
    paths: Sequence[str],
    collection_ops: Iterable[OpDef],
    version: int,
    warn: bool = True,
) -> None:
    client = LDBClient(ldb_dir)
    version_msg = "latest version" if version == -1 else f"v{version}"
    ds_name = load_workspace_dataset(workspace_path).dataset_name
    ds_ident = format_dataset_identifier(ds_name)
    ws_data_object_hashes = set(
        get_collection_dir_keys(workspace_path / WorkspacePath.COLLECTION),
    )
    data_object_hashes, annotation_hashes, _, _ = process_args_for_ls(
        client,
        paths,
    )
    collection: Iterator[Tuple[str, str]] = (
        (d, a)
        for d, a in zip(data_object_hashes, annotation_hashes)
        if d in ws_data_object_hashes
    )
    collection = apply_queries_to_collection(
        client,
        collection,
        collection_ops,
        warn=warn,
    )
    data_object_hashes = (d for d, _ in collection)
    print(f"Updating to {version_msg}")
    updates, num_missing = get_collection_with_updated_annotations(
        client,
        data_object_hashes,
        version,
    )
    num_updated_annots, num_already_updated = update_annotation_versions(
        workspace_path,
        updates,
    )
    print(
        "\n"
        f"{ds_ident}\n"
        f"  Already up-to-date: {num_already_updated:8d}\n"
        f"  New updates:        {num_updated_annots:8d}\n"
        f"  Missing version:    {num_missing:8d}\n",
    )


def get_hash_annot_pair_version(
    hash_annot_pair: Tuple[str, JSONObject],
) -> int:
    return hash_annot_pair[1]["version"]  # type: ignore[no-any-return]


def get_collection_with_updated_annotations(
    client: "LDBClient",
    data_object_hashes: Iterable[str],
    version: int = -1,
) -> Tuple[List[Tuple[str, str]], int]:
    result = []
    num_missing = 0
    for id, annot_id in client.db.get_annotation_version_hashes(data_object_hashes, version):
        if annot_id:
            result.append((id, annot_id))
        else:
            num_missing += 1
    return result, num_missing


def update_annotation_versions(
    workspace_path: Path,
    collection: Iterable[Tuple[str, str]],
) -> Tuple[int, int]:
    workspace_path = Path(os.path.normpath(workspace_path))
    collection_dir_path = workspace_path / WorkspacePath.COLLECTION

    to_write = []
    for data_object_hash, annotation_hash in collection:
        to_write.append(
            (
                get_hash_path(collection_dir_path, data_object_hash),
                annotation_hash,
            ),
        )

    num_updated_annots = 0
    num_already_updated = 0
    for collection_member_path, annotation_hash in to_write:
        with collection_member_path.open("r+") as file:
            existing_annotation_hash = file.read()
            if annotation_hash == existing_annotation_hash:
                num_already_updated += 1
            else:
                file.seek(0)
                file.write(annotation_hash)
                file.truncate()
                num_updated_annots += 1
    return num_updated_annots, num_already_updated

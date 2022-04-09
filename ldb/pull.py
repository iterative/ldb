import os
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

from ldb.add import process_args_for_ls
from ldb.dataset import OpDef, apply_queries
from ldb.path import InstanceDir, WorkspacePath
from ldb.typing import JSONObject
from ldb.utils import format_dataset_identifier, get_hash_path, load_data_file
from ldb.workspace import load_workspace_dataset


def pull(
    ldb_dir: Path,
    workspace_path: Path,
    paths: Sequence[str],
    collection_ops: Iterable[OpDef],
) -> None:
    data_object_hashes, annotation_hashes, _ = process_args_for_ls(
        ldb_dir,
        paths,
    )
    collection = apply_queries(
        ldb_dir,
        data_object_hashes,
        annotation_hashes,
        collection_ops,
    )
    data_object_hashes = (d for d, _ in collection)
    updates = get_collection_with_updated_annotations(
        ldb_dir,
        data_object_hashes,
    )
    return update_annotation_versions(workspace_path, updates)


def get_hash_annot_pair_version(
    hash_annot_pair: Tuple[str, JSONObject],
) -> int:
    return hash_annot_pair[1]["version"]  # type: ignore[no-any-return]


def get_annotation_version_hash(
    ldb_dir: Path,
    data_object_hash: str,
    version: int,
) -> str:
    data_object_dir = get_hash_path(
        ldb_dir / InstanceDir.DATA_OBJECT_INFO,
        data_object_hash,
    )
    if (data_object_dir / "annotations").is_dir():
        annotation_metas = [
            (f.name, load_data_file(f))
            for f in (data_object_dir / "annotations").iterdir()
        ]
        annotation_metas.sort(key=get_hash_annot_pair_version)
        try:
            return annotation_metas[version][0]
        except IndexError:
            pass
    return ""


def get_collection_with_updated_annotations(
    ldb_dir: Path,
    data_object_hashes: Iterable[str],
    version: int = -1,
) -> List[Tuple[str, str]]:
    result = []
    for data_object_hash in data_object_hashes:
        new_annotation_hash = get_annotation_version_hash(
            ldb_dir,
            data_object_hash,
            version,
        )
        if new_annotation_hash:
            result.append((data_object_hash, new_annotation_hash))
    return result


def update_annotation_versions(
    workspace_path: Path,
    collection: Iterable[Tuple[str, str]],
) -> None:
    workspace_path = Path(os.path.normpath(workspace_path))
    ds_name = load_workspace_dataset(workspace_path).dataset_name
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
    for collection_member_path, annotation_hash in to_write:
        with collection_member_path.open("r+") as file:
            existing_annotation_hash = file.read()
            if annotation_hash != existing_annotation_hash:
                file.seek(0)
                file.write(annotation_hash)
                file.truncate()
                num_updated_annots += 1
    ds_ident = format_dataset_identifier(ds_name)
    print(f"Pulled {num_updated_annots} annotations for {ds_ident}")

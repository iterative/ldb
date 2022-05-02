from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from ldb.add import process_args_for_ls
from ldb.dataset import OpDef, apply_queries
from ldb.path import InstanceDir
from ldb.string_utils import left_truncate
from ldb.utils import DATA_OBJ_ID_PREFIX, get_hash_path, load_data_file


@dataclass
class DatasetListing:
    data_object_hash: str
    data_object_path: str
    annotation_hash: str
    annotation_version: int


def ls(
    ldb_dir: Path,
    paths: Sequence[str],
    collection_ops: Iterable[OpDef],
    warn: bool = True,
) -> List[DatasetListing]:
    data_object_hashes, annotation_hashes, _ = process_args_for_ls(
        ldb_dir,
        paths,
    )
    collection = apply_queries(
        ldb_dir,
        data_object_hashes,
        annotation_hashes,
        collection_ops,
        warn=warn,
    )
    return ls_collection(ldb_dir, collection)


def print_dataset_listings(
    dataset_listings: List[DatasetListing],
    verbose: bool = False,
) -> int:
    if not dataset_listings:
        return 0

    num_items = 0
    print(f"{'Data Object Hash':37} {'Annot.':8} Data Object Path")
    for item in dataset_listings:
        annotation_version = str(item.annotation_version or "-")
        path = (
            item.data_object_path
            if verbose
            else left_truncate(item.data_object_path)
        )
        print(
            f"  {DATA_OBJ_ID_PREFIX}{item.data_object_hash:35} "
            f"{annotation_version:8} {path}",
        )
        num_items += 1
    return num_items


def ls_collection(
    ldb_dir: Path,
    collection: Iterable[Tuple[str, Optional[str]]],
) -> List[DatasetListing]:
    result = []
    data_object_info_path = ldb_dir / InstanceDir.DATA_OBJECT_INFO
    for data_object_hash, annotation_hash in collection:
        data_object_dir = get_hash_path(
            data_object_info_path,
            data_object_hash,
        )
        annotation_version = 0
        if annotation_hash:
            annotation_meta = load_data_file(
                data_object_dir / "annotations" / annotation_hash,
            )
            annotation_version = annotation_meta["version"]
        data_object_meta = load_data_file(data_object_dir / "meta")

        result.append(
            DatasetListing(
                data_object_hash=data_object_hash,
                data_object_path=data_object_meta["fs"]["path"],
                annotation_hash=annotation_hash or "",
                annotation_version=annotation_version,
            ),
        )
    return result

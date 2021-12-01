from dataclasses import dataclass
from pathlib import Path
from typing import List, Mapping, Optional, Sequence

from ldb.add import apply_queries, process_args_for_ls, process_bool_query_args
from ldb.path import InstanceDir
from ldb.string_utils import left_truncate
from ldb.utils import get_hash_path, load_data_file


@dataclass
class DatasetListing:
    data_object_hash: str
    data_object_path: str
    annotation_hash: str
    annotation_version: int


def ls(
    ldb_dir: Path,
    paths: Sequence[str],
    annotation_query: Optional[str] = None,
    file_query: Optional[str] = None,
) -> List[DatasetListing]:
    search, file_search = process_bool_query_args(annotation_query, file_query)
    data_object_hashes, annotation_hashes, _ = process_args_for_ls(
        ldb_dir,
        paths,
    )
    collection = apply_queries(
        ldb_dir,
        search,
        file_search,
        data_object_hashes,
        annotation_hashes,
    )
    return ls_collection(ldb_dir, collection)


def print_dataset_listings(
    dataset_listings: List[DatasetListing],
    verbose: bool = False,
) -> None:
    print(f"{'Data Object Hash':37} {'Annot.':8} Data Object Path")
    for item in dataset_listings:
        annotation_version = str(item.annotation_version or "-")
        path = (
            item.data_object_path
            if verbose
            else left_truncate(item.data_object_path)
        )
        print(f"  0x{item.data_object_hash:35} {annotation_version:8} {path}")


def ls_collection(
    ldb_dir: Path,
    collection: Mapping[str, Optional[str]],
) -> List[DatasetListing]:
    result = []
    for data_object_hash, annotation_hash in collection.items():
        data_object_dir = get_hash_path(
            ldb_dir / InstanceDir.DATA_OBJECT_INFO,
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

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Generator, Iterable, Optional, Tuple

from ldb.dataset import Dataset, get_collection
from ldb.exceptions import LDBException
from ldb.path import InstanceDir, WorkspacePath
from ldb.utils import (
    format_dataset_identifier,
    get_hash_path,
    load_data_file,
    parse_dataset_identifier,
)
from ldb.workspace import collection_dir_to_object


@dataclass
class DiffItem:
    data_object_hash: str
    annotation_hash1: str
    annotation_hash2: str
    data_object_path: str = ""
    annotation_version1: int = 0
    annotation_version2: int = 0


def diff(
    ldb_dir,
    workspace_path,
    dataset1,
    dataset2=None,
) -> Generator[DiffItem, None, None]:
    if dataset1.startswith("ds:"):
        collection1 = get_collection(
            ldb_dir,
            get_dataset_version_hash(ldb_dir, dataset1),
        )
    elif dataset1:
        collection1 = get_collection(
            ldb_dir,
            dataset1,
        )
    else:
        collection1 = {}

    if dataset2 is not None:
        collection2 = get_collection(
            ldb_dir,
            get_dataset_version_hash(ldb_dir, dataset2),
        )
    else:
        collection2 = collection_dir_to_object(
            workspace_path / WorkspacePath.COLLECTION,
        )
    return add_meta_to_simple_diff(
        ldb_dir,
        simple_diff(collection1, collection2),
    )


def summarize_diff(diff_items: Iterable[DiffItem]) -> Tuple[int, int, int]:
    additions = 0
    deletions = 0
    modifications = 0
    for diff_item in diff_items:
        if diff_item.annotation_version2 and not diff_item.annotation_version1:
            additions += 1
        elif (
            not diff_item.annotation_version2 and diff_item.annotation_version1
        ):
            deletions += 1
        elif diff_item.annotation_version1 != diff_item.annotation_version2:
            modifications += 1
    return additions, deletions, modifications


def get_dataset_version_hash(ldb_dir: Path, dataset_identifier: str) -> str:
    ds_name, ds_version_num = parse_dataset_identifier(dataset_identifier)
    dataset_obj = Dataset.parse(
        load_data_file(ldb_dir / InstanceDir.DATASETS / ds_name),
    )
    if ds_version_num is None:
        return dataset_obj.versions[-1]
    try:
        return dataset_obj.versions[ds_version_num - 1]
    except IndexError as exc:
        latest_dataset = format_dataset_identifier(
            ds_name,
            len(dataset_obj.versions),
        )
        raise LDBException(
            f"{dataset_identifier} does not exist\n"
            f"The latest version is {latest_dataset}",
        ) from exc


def add_meta_to_simple_diff(
    ldb_dir: Path,
    simple_diff_gen: Generator[Tuple[str, str, str], None, None],
) -> Generator[DiffItem, None, None]:
    for (
        data_object_hash,
        annotation_hash1,
        annotation_hash2,
    ) in simple_diff_gen:
        annotation_version1 = get_annotation_version(
            ldb_dir,
            data_object_hash,
            annotation_hash1,
        )
        annotation_version2 = get_annotation_version(
            ldb_dir,
            data_object_hash,
            annotation_hash2,
        )
        data_object_meta = load_data_file(
            get_hash_path(
                ldb_dir / InstanceDir.DATA_OBJECT_INFO,
                data_object_hash,
            )
            / "meta",
        )
        yield DiffItem(
            data_object_hash=data_object_hash,
            annotation_hash1=annotation_hash1,
            annotation_hash2=annotation_hash2,
            data_object_path=data_object_meta["fs"]["path"],
            annotation_version1=annotation_version1,
            annotation_version2=annotation_version2,
        )


def get_annotation_version(
    ldb_dir: Path,
    data_object_hash: str,
    annotation_hash: str,
) -> int:
    if annotation_hash:
        annotation_meta = load_data_file(
            get_hash_path(
                ldb_dir / InstanceDir.DATA_OBJECT_INFO,
                data_object_hash,
            )
            / "annotations"
            / annotation_hash,
        )
        return annotation_meta["version"]
    return 0


def simple_diff(
    collection1: Dict[str, Optional[str]],
    collection2: Dict[str, Optional[str]],
) -> Generator[Tuple[str, str, str], None, None]:
    iter1 = iter(collection1.items())
    iter2 = iter(collection2.items())
    cont = True
    try:
        data_object_hash1, annotation_hash1 = next(iter1)
        data_object_hash2, annotation_hash2 = next(iter2)
    except StopIteration:
        cont = False
    while cont:
        try:
            if data_object_hash1 < data_object_hash2:
                yield data_object_hash1, annotation_hash1 or "", ""
                data_object_hash1, annotation_hash1 = next(iter1)
            elif data_object_hash1 > data_object_hash2:
                yield data_object_hash2, "", annotation_hash2 or ""
                data_object_hash2, annotation_hash2 = next(iter2)
            else:
                yield (
                    data_object_hash1,
                    annotation_hash1 or "",
                    annotation_hash2 or "",
                )
                data_object_hash1, annotation_hash1 = next(iter1)
                data_object_hash2, annotation_hash2 = next(iter2)
        except StopIteration:
            cont = False
    for data_object_hash1, annotation_hash1 in iter1:
        yield data_object_hash1, annotation_hash1 or "", ""
    for data_object_hash2, annotation_hash2 in iter2:
        yield data_object_hash2, "", annotation_hash2 or ""

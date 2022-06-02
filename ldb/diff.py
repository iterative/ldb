from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional, Tuple

from ldb.dataset import Dataset, get_collection
from ldb.exceptions import LDBException
from ldb.path import InstanceDir, WorkspacePath
from ldb.utils import (
    DATASET_PREFIX,
    WORKSPACE_DATASET_PREFIX,
    format_dataset_identifier,
    get_hash_path,
    load_data_file,
    parse_dataset_identifier,
)
from ldb.workspace import collection_dir_to_object, load_workspace_dataset


class DiffType(IntEnum):
    SAME = 1
    ADDITION = 2
    DELETION = 3
    MODIFICATION = 4


@dataclass
class SimpleDiffItem:
    data_object_hash: str
    annotation_hash1: str
    annotation_hash2: str
    diff_type: DiffType


@dataclass
class DiffItem(SimpleDiffItem):
    data_object_path: str
    annotation_version1: int
    annotation_version2: int


def get_diff_collection(
    ldb_dir: Path,
    dataset: str,
) -> Dict[str, Optional[str]]:
    if dataset.startswith(DATASET_PREFIX):
        return get_collection(
            ldb_dir,
            get_dataset_version_hash(ldb_dir, dataset),
        )
    if dataset.startswith(WORKSPACE_DATASET_PREFIX):
        return collection_dir_to_object(
            Path(dataset[len(WORKSPACE_DATASET_PREFIX) :])
            / WorkspacePath.COLLECTION,
        )
    return get_collection(ldb_dir, dataset)


def get_diff_collections(
    ldb_dir: Path,
    dataset1: str = "",
    dataset2: str = "",
    workspace_path: str = ".",
) -> Tuple[Dict[str, Optional[str]], Dict[str, Optional[str]]]:
    if dataset1:
        collection1 = get_diff_collection(ldb_dir, dataset1)
        if dataset2:
            return collection1, get_diff_collection(ldb_dir, dataset2)
    elif dataset2:
        raise Exception
    else:
        workspace_dataset = load_workspace_dataset(Path(workspace_path))
        if workspace_dataset.parent:
            collection1 = get_collection(ldb_dir, workspace_dataset.parent)
        else:
            collection1 = {}
    collection2 = collection_dir_to_object(
        Path(workspace_path) / WorkspacePath.COLLECTION,
    )
    return collection1, collection2


def diff(
    ldb_dir: Path,
    dataset1: str = "",
    dataset2: str = "",
    workspace_path: str = ".",
) -> Iterator[DiffItem]:
    return full_diff(
        ldb_dir,
        simple_diff(
            ldb_dir,
            dataset1,
            dataset2,
            workspace_path,
        ),
    )


def summarize_diff(
    diff_items: Iterable[SimpleDiffItem],
) -> Tuple[int, int, int]:
    additions = 0
    deletions = 0
    modifications = 0
    for item in diff_items:
        if item.diff_type == DiffType.ADDITION:
            additions += 1
        elif item.diff_type == DiffType.DELETION:
            deletions += 1
        elif item.diff_type == DiffType.MODIFICATION:
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


def full_diff(
    ldb_dir: Path,
    simple_diff_items: Iterable[SimpleDiffItem],
) -> Iterator[DiffItem]:
    data_object_info_path = ldb_dir / InstanceDir.DATA_OBJECT_INFO
    for item in simple_diff_items:
        annotation_version1 = get_annotation_version(
            ldb_dir,
            item.data_object_hash,
            item.annotation_hash1,
        )
        if item.diff_type == DiffType.SAME:
            annotation_version2 = annotation_version1
        else:
            annotation_version2 = get_annotation_version(
                ldb_dir,
                item.data_object_hash,
                item.annotation_hash2,
            )
        data_object_meta = load_data_file(
            get_hash_path(
                data_object_info_path,
                item.data_object_hash,
            )
            / "meta",
        )
        yield DiffItem(
            data_object_hash=item.data_object_hash,
            annotation_hash1=item.annotation_hash1,
            annotation_hash2=item.annotation_hash2,
            diff_type=item.diff_type,
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
        return annotation_meta["version"]  # type: ignore[no-any-return]
    return 0


def simple_diff(
    ldb_dir: Path,
    dataset1: str = "",
    dataset2: str = "",
    workspace_path: str = ".",
) -> Iterator[SimpleDiffItem]:
    collection1, collection2 = get_diff_collections(
        ldb_dir,
        dataset1,
        dataset2,
        workspace_path,
    )
    return simple_diff_on_collections(collection1, collection2)


def simple_diff_on_collections(
    collection1: Dict[str, Optional[str]],
    collection2: Dict[str, Optional[str]],
) -> Iterator[SimpleDiffItem]:
    # pylint: disable=invalid-name
    iter1 = iter(sorted(collection1.items()))
    iter2 = iter(sorted(collection2.items()))

    d1, a1 = next(iter1, (None, None))
    d2, a2 = next(iter2, (None, None))
    while d1 is not None and d2 is not None:
        if d1 < d2:
            yield SimpleDiffItem(d1, a1 or "", "", DiffType.DELETION)
            d1, a1 = next(iter1, (None, None))
        elif d1 > d2:
            yield SimpleDiffItem(d2, "", a2 or "", DiffType.ADDITION)
            d2, a2 = next(iter2, (None, None))
        else:
            diff_type = DiffType.SAME if a1 == a2 else DiffType.MODIFICATION
            yield SimpleDiffItem(d1, a1 or "", a2 or "", diff_type)
            d1, a1 = next(iter1, (None, None))
            d2, a2 = next(iter2, (None, None))

    if d1 is None:
        if d2 is not None:
            yield SimpleDiffItem(d2, "", a2 or "", DiffType.ADDITION)
    elif d2 is None:
        yield SimpleDiffItem(d1, a1 or "", "", DiffType.DELETION)

    for d1, a1 in iter1:
        yield SimpleDiffItem(d1, a1 or "", "", DiffType.DELETION)
    for d2, a2 in iter2:
        yield SimpleDiffItem(d2, "", a2 or "", DiffType.ADDITION)


def format_summary(additions: int, deletions: int, modifications: int) -> str:
    return (
        f"  Additions (+): {additions:8}\n"
        f"  Deletions (-): {deletions:8}\n"
        f"  Modifications (m): {modifications:4}"
    )

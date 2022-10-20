from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional, Tuple

from ldb.core import LDBClient
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
    client: "LDBClient",
    dataset: str,
) -> Dict[str, Optional[str]]:
    if dataset.startswith(DATASET_PREFIX):
        name, version_num = parse_dataset_identifier(dataset)
        ds_version_obj = client.db.get_dataset_version_by_name(name, version_num)[0]
        return dict(client.db.get_collection(ds_version_obj.collection))
    if dataset.startswith(WORKSPACE_DATASET_PREFIX):
        return collection_dir_to_object(
            Path(dataset[len(WORKSPACE_DATASET_PREFIX) :]) / WorkspacePath.COLLECTION,
        )
    ds_version_obj = client.db.get_dataset_version(dataset)
    return dict(client.db.get_collection(ds_version_obj.collection))


def get_diff_collections(
    client: "LDBClient",
    dataset1: str = "",
    dataset2: str = "",
    workspace_path: str = ".",
) -> Tuple[Dict[str, Optional[str]], Dict[str, Optional[str]]]:
    if dataset1:
        collection1 = get_diff_collection(client, dataset1)
        if dataset2:
            return collection1, get_diff_collection(client, dataset2)
    elif dataset2:
        raise Exception
    else:
        workspace_dataset = load_workspace_dataset(Path(workspace_path))
        if workspace_dataset.parent:
            ds_version_obj = client.db.get_dataset_version(workspace_dataset.parent)
            collection1 = dict(client.db.get_collection(ds_version_obj.collection))
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
    client = LDBClient(ldb_dir)
    return full_diff(
        client,
        simple_diff(
            client,
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


def full_diff(
    client: "LDBClient",
    simple_diff_items: Iterable[SimpleDiffItem],
) -> Iterator[DiffItem]:
    for item in simple_diff_items:
        annotation_version1 = get_annotation_version(
            client,
            item.data_object_hash,
            item.annotation_hash1,
        )
        if item.diff_type == DiffType.SAME:
            annotation_version2 = annotation_version1
        else:
            annotation_version2 = get_annotation_version(
                client,
                item.data_object_hash,
                item.annotation_hash2,
            )
        data_object_meta = client.db.get_data_object_meta(item.data_object_hash)[1]
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
    client: "LDBClient",
    data_object_hash: str,
    annotation_hash: str,
) -> int:
    if annotation_hash:
        meta = client.db.get_pair_meta(data_object_hash, annotation_hash)[2]
        return meta["version"]  # type: ignore[no-any-return]
    return 0


def simple_diff(
    client: "LDBClient",
    dataset1: str = "",
    dataset2: str = "",
    workspace_path: str = ".",
) -> Iterator[SimpleDiffItem]:
    collection1, collection2 = get_diff_collections(
        client,
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

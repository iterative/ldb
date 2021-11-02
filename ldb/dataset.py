import os
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ldb.exceptions import LDBException
from ldb.path import InstanceDir, WorkspacePath
from ldb.utils import (
    format_datetime,
    get_hash_path,
    load_data_file,
    parse_datetime,
)


@dataclass
class CommitInfo:
    created_by: str
    commit_time: datetime
    commit_message: str

    @classmethod
    def parse(cls, attr_dict: Dict[str, str]) -> "CommitInfo":
        attr_dict = attr_dict.copy()
        commit_time = parse_datetime(attr_dict.pop("commit_time"))
        return cls(commit_time=commit_time, **attr_dict)

    def format(self) -> Dict[str, str]:
        attr_dict = asdict(self)
        commit_time = format_datetime(attr_dict.pop("commit_time"))
        return dict(commit_time=commit_time, **attr_dict)


@dataclass
class DatasetVersion:
    version: int
    parent: Optional[str]
    collection: str
    tags: List[str]
    commit_info: CommitInfo

    @classmethod
    def parse(cls, attr_dict: Dict[str, Any]) -> "DatasetVersion":
        attr_dict = attr_dict.copy()
        commit_info = CommitInfo.parse(attr_dict.pop("commit_info"))
        return cls(commit_info=commit_info, **attr_dict)

    def format(self) -> Dict[str, Any]:
        attr_dict = {f.name: getattr(self, f.name) for f in fields(self)}
        commit_info = attr_dict.pop("commit_info").format()
        tags = attr_dict.pop("tags").copy()
        return dict(commit_info=commit_info, tags=tags, **attr_dict)


@dataclass
class Dataset:
    name: str
    created_by: str
    created: datetime
    versions: List[str]

    @classmethod
    def parse(cls, attr_dict: Dict[str, Any]) -> "Dataset":
        attr_dict = attr_dict.copy()
        created = parse_datetime(attr_dict.pop("created"))
        return cls(created=created, **attr_dict)

    def format(self) -> Dict[str, Any]:
        attr_dict = asdict(self)
        created = format_datetime(attr_dict.pop("created"))
        return dict(created=created, **attr_dict)


def get_workspace_dataset(workspace_path: Path) -> Dict[str, Any]:
    try:
        return load_data_file(workspace_path / WorkspacePath.DATASET)
    except FileNotFoundError as exc:
        raise LDBException(
            "No workspace dataset staged at "
            f"{repr(os.fspath(workspace_path))}",
        ) from exc


def workspace_dataset_is_clean(ldb_dir, workspace_dataset_obj, workspace_path):
    parent = workspace_dataset_obj["parent"]
    ws_collection = collection_dir_to_object(
        workspace_path / WorkspacePath.COLLECTION,
    )
    if parent is None:
        return not ws_collection
    collection_obj = get_collection(ldb_dir, parent)
    return ws_collection == collection_obj


def collection_dir_to_object(collection_dir: Path) -> Dict[str, Optional[str]]:
    items = []
    for path in collection_dir.glob("*/*"):
        data_object_hash = path.parent.name + path.name
        with path.open() as file:
            annotation_hash = file.read()
        items.append((data_object_hash, annotation_hash or None))
    items.sort()
    return dict(items)


def get_collection(ldb_dir, dataset_version_hash):
    dataset_version_obj = DatasetVersion.parse(
        load_data_file(
            get_hash_path(
                ldb_dir / InstanceDir.DATASET_VERSIONS,
                dataset_version_hash,
            ),
        ),
    )
    return load_data_file(
        get_hash_path(
            ldb_dir / InstanceDir.COLLECTIONS,
            dataset_version_obj.collection,
        ),
    )

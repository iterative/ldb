import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ldb.dataset import get_collection
from ldb.exceptions import LDBException
from ldb.path import WorkspacePath
from ldb.utils import format_datetime, load_data_file, parse_datetime


@dataclass
class WorkspaceDataset:
    dataset_name: str
    staged_time: datetime
    parent: str
    tags: List[str]

    @classmethod
    def parse(cls, attr_dict: Dict[str, Any]) -> "WorkspaceDataset":
        attr_dict = attr_dict.copy()
        return cls(
            staged_time=parse_datetime(attr_dict.pop("staged_time")),
            tags=attr_dict.pop("tags").copy(),
            **attr_dict,
        )

    def format(self) -> Dict[str, Any]:
        attr_dict = asdict(self)
        return dict(
            staged_time=format_datetime(attr_dict.pop("staged_time")),
            tags=attr_dict.pop("tags").copy(),
            **attr_dict,
        )


def workspace_dataset_is_clean(
    ldb_dir: Path,
    workspace_dataset_obj: WorkspaceDataset,
    workspace_path: Path,
) -> bool:
    ws_collection = collection_dir_to_object(
        workspace_path / WorkspacePath.COLLECTION,
    )
    if not workspace_dataset_obj.parent:
        return not ws_collection
    collection_obj = get_collection(ldb_dir, workspace_dataset_obj.parent)
    return ws_collection == collection_obj


def load_workspace_dataset(workspace_path: Path) -> WorkspaceDataset:
    try:
        return WorkspaceDataset.parse(
            load_data_file(workspace_path / WorkspacePath.DATASET),
        )
    except FileNotFoundError as exc:
        raise LDBException(
            "No workspace dataset staged at "
            f"{repr(os.fspath(workspace_path))}",
        ) from exc


def collection_dir_to_object(collection_dir: Path) -> Dict[str, Optional[str]]:
    items = []
    for path in collection_dir.glob("*/*"):
        data_object_hash = path.parent.name + path.name
        annotation_hash = path.read_text() or None
        items.append((data_object_hash, annotation_hash))
    items.sort()
    return dict(items)

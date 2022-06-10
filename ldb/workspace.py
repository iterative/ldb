import os
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

from ldb.dataset import DatasetVersion, get_collection_dir_items
from ldb.exceptions import WorkspaceDatasetNotFoundError, WorkspaceError
from ldb.path import InstanceDir, WorkspacePath
from ldb.utils import (
    DATASET_PREFIX,
    ROOT,
    format_datetime,
    get_hash_path,
    load_data_file,
    parse_datetime,
)


@dataclass
class WorkspaceDataset:
    dataset_name: str
    staged_time: datetime
    parent: str
    tags: List[str]
    auto_pull: bool = False

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
    dataset_version_obj = DatasetVersion.from_id(
        ldb_dir,
        workspace_dataset_obj.parent,
    )
    collection_obj = load_data_file(
        get_hash_path(
            ldb_dir / InstanceDir.COLLECTIONS,
            dataset_version_obj.collection,
        ),
    )
    if ws_collection != collection_obj:
        return False

    from ldb.transform import (  # pylint: disable=import-outside-toplevel
        transform_dir_to_object,
    )

    ws_transform_mapping = transform_dir_to_object(
        workspace_path / WorkspacePath.TRANSFORM_MAPPING,
    )
    transform_obj: Dict[str, List[str]] = load_data_file(
        get_hash_path(
            ldb_dir / InstanceDir.TRANSFORM_MAPPINGS,
            dataset_version_obj.transform_mapping_id,
        ),
    )
    return ws_transform_mapping == transform_obj


def load_workspace_dataset(workspace_path: Path) -> WorkspaceDataset:
    try:
        workspace_ds = WorkspaceDataset.parse(
            load_data_file(workspace_path / WorkspacePath.DATASET),
        )
    except FileNotFoundError as exc:
        raise WorkspaceDatasetNotFoundError(
            "No workspace dataset staged at "
            f"{repr(os.fspath(workspace_path))}",
        ) from exc
    if workspace_ds.dataset_name == ROOT:
        raise ValueError(
            "Invalid workspace dataset name: "
            f"{DATASET_PREFIX}{workspace_ds.dataset_name}",
        )
    return workspace_ds


def collection_dir_to_object(collection_dir: Path) -> Dict[str, Optional[str]]:
    return dict(
        sorted(get_collection_dir_items(collection_dir, is_workspace=True)),
    )


def iter_workspace_dir(
    workspace_path: Union[str, Path],
) -> Iterator[os.DirEntry]:  # type: ignore[type-arg]
    ldb_workspace_name = WorkspacePath.BASE.name
    for entry in os.scandir(workspace_path):
        if entry.name != ldb_workspace_name or not entry.is_dir():
            yield entry


def ensure_path_is_empty_workspace(
    path: Union[str, Path],
    force: bool = False,
) -> None:
    if any(iter_workspace_dir(path)):
        try:
            load_workspace_dataset(Path(path))
        except WorkspaceDatasetNotFoundError as exc:
            raise WorkspaceDatasetNotFoundError(
                f"Not a workspace or an empty directory: {os.fspath(path)}",
            ) from exc
        if force:
            remove_workspace_contents(path)
        else:
            raise WorkspaceError(
                "Workspace is not empty: "
                f"{os.fspath(path)!r}\n"
                "Use the --force option to delete workspace contents",
            )


def remove_workspace_contents(workspace_path: Union[str, Path]) -> None:
    for entry in iter_workspace_dir(workspace_path):
        if entry.is_dir():
            shutil.rmtree(entry)
        else:
            os.remove(entry)

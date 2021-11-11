import os
from dataclasses import dataclass
from pathlib import Path

from ldb.dataset import get_collection_dir_items
from ldb.exceptions import LDBException
from ldb.path import InstanceDir, WorkspacePath
from ldb.workspace import load_workspace_dataset


@dataclass
class WorkspaceStatus:
    dataset_name: str
    num_data_objects: int
    num_annotations: int


def status(ldb_dir: Path, workspace_path: Path):
    workspace_path = Path(os.path.normpath(workspace_path))
    try:
        workspace_ds = load_workspace_dataset(workspace_path)
    except LDBException:
        ds_name = "root"
        item_gen = get_collection_dir_items(
            ldb_dir / InstanceDir.DATA_OBJECT_INFO,
            is_workspace=False,
        )
    else:
        ds_name = workspace_ds.dataset_name
        item_gen = get_collection_dir_items(
            workspace_path / WorkspacePath.COLLECTION,
            is_workspace=True,
        )

    num_data_objects = 0
    num_annotations = 0
    for _, annotation_hash in item_gen:
        num_data_objects += 1
        num_annotations += bool(annotation_hash)
    return WorkspaceStatus(
        dataset_name=ds_name,
        num_data_objects=num_data_objects,
        num_annotations=num_annotations,
    )

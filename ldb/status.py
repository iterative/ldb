import os
from dataclasses import dataclass
from pathlib import Path

from ldb.dataset import get_root_collection_items
from ldb.exceptions import LDBException
from ldb.path import WorkspacePath
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
        item_gen = get_root_collection_items(ldb_dir)
        ds_name = "root"
    else:
        item_gen = get_collection_dir_items(workspace_path)
        ds_name = workspace_ds.dataset_name

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


def get_collection_dir_items(workspace_path: Path):
    for path in (workspace_path / WorkspacePath.COLLECTION).glob("*/*"):
        data_object_hash = path.parent.name + path.name
        annotation_hash = path.read_text()
        yield data_object_hash, annotation_hash

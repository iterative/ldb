from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple

from ldb.dataset import (
    DatasetVersion,
    get_collection_dir_items,
    get_dataset,
    get_dataset_version_hash,
)
from ldb.path import InstanceDir, WorkspacePath
from ldb.utils import (
    ROOT,
    get_hash_path,
    load_data_file,
    parse_dataset_identifier,
)
from ldb.workspace import load_workspace_dataset


@dataclass
class WorkspaceStatus:
    dataset_name: str
    dataset_version: int
    num_data_objects: int
    num_annotations: int
    auto_pull: bool


def status(ldb_dir: Path, dataset: str) -> WorkspaceStatus:
    item_gen: Iterable[Tuple[str, Optional[str]]]
    if not dataset:
        workspace_path = Path(".")
        workspace_ds = load_workspace_dataset(workspace_path)
        ds_name = workspace_ds.dataset_name
        ds_version = 0
        item_gen = get_collection_dir_items(
            workspace_path / WorkspacePath.COLLECTION,
            is_workspace=True,
        )
        auto_pull = workspace_ds.auto_pull
    else:
        ds_name, opt_ds_version = parse_dataset_identifier(dataset)
        ds_version = opt_ds_version or 0
        if ds_name == ROOT:
            item_gen = get_collection_dir_items(
                ldb_dir / InstanceDir.DATA_OBJECT_INFO,
                is_workspace=False,
            )
            auto_pull = False
        else:
            dataset_obj = get_dataset(ldb_dir, ds_name)
            if not ds_version:
                ds_version = len(dataset_obj.versions)
            dataset_version_hash = get_dataset_version_hash(
                dataset_obj,
                ds_version,
            )
            dataset_version_obj = DatasetVersion.parse(
                load_data_file(
                    get_hash_path(
                        ldb_dir / InstanceDir.DATASET_VERSIONS,
                        dataset_version_hash,
                    ),
                ),
            )
            item_gen = load_data_file(
                get_hash_path(
                    ldb_dir / InstanceDir.COLLECTIONS,
                    dataset_version_obj.collection,
                ),
            ).items()
            auto_pull = dataset_version_obj.auto_pull

    num_data_objects = 0
    num_annotations = 0
    for _, annotation_hash in item_gen:
        num_data_objects += 1
        num_annotations += bool(annotation_hash)
    return WorkspaceStatus(
        dataset_name=ds_name,
        dataset_version=ds_version,
        num_data_objects=num_data_objects,
        num_annotations=num_annotations,
        auto_pull=auto_pull,
    )

import getpass
import os
from pathlib import Path
from typing import Optional

from ldb.dataset import CommitInfo, Dataset
from ldb.objects.collection import CollectionObject
from ldb.objects.dataset_version import DatasetVersion
from ldb.objects.transform_mapping import TransformMapping
from ldb.path import InstanceDir, WorkspacePath
from ldb.transform import transform_dir_to_object
from ldb.utils import (
    DATASET_PREFIX,
    current_time,
    format_dataset_identifier,
    json_dumps,
    load_data_file,
    parse_dataset_identifier,
    write_data_file,
)
from ldb.workspace import (
    collection_dir_to_object,
    load_workspace_dataset,
    workspace_dataset_is_clean,
)


def commit(
    ldb_dir: Path,
    workspace_path: Path,
    dataset_identifier: str = "",
    message: str = "",
    auto_pull: Optional[bool] = None,
) -> None:
    from ldb.core import LDBClient

    client = LDBClient(ldb_dir)
    workspace_path = Path(os.path.normpath(workspace_path))
    workspace_ds = load_workspace_dataset(workspace_path)
    if not dataset_identifier:
        dataset_name = workspace_ds.dataset_name
        dataset_identifier = format_dataset_identifier(dataset_name)
    if dataset_identifier.startswith(f"{DATASET_PREFIX}.temp."):
        raise ValueError(
            f"Cannot commit {dataset_identifier}\n"
            f'Names beginning with "{DATASET_PREFIX}.temp." are temporary '
            "dataset names. Please specify a different dataset name.",
        )
    dataset_name, version_num = parse_dataset_identifier(
        dataset_identifier,
    )
    if version_num is not None:
        raise ValueError("Dataset name cannot include version when committing")

    if workspace_dataset_is_clean(
        client,
        workspace_ds,
        workspace_path,
    ):
        print("Nothing to commit.")
        return
    collection_obj = CollectionObject(
        collection_dir_to_object(workspace_path / WorkspacePath.COLLECTION)
    )
    collection_obj.digest()
    client.db.check_for_missing_data_object_ids(collection_obj.keys())
    client.db.add_collection(collection_obj)

    # transform_hash = save_transform_object(ldb_dir, workspace_path)
    transform_mapping = TransformMapping(
        transform_dir_to_object(workspace_path / WorkspacePath.TRANSFORM_MAPPING)
    )
    transform_mapping.digest()
    client.db.add_transform_mapping(transform_mapping)

    curr_time = current_time()
    username = getpass.getuser()

    dataset_file_path = ldb_dir / InstanceDir.DATASETS / dataset_name
    try:
        num_versions = len(Dataset.parse(load_data_file(dataset_file_path)).versions)
    except FileNotFoundError:
        num_versions = 0
    auto_pull = workspace_ds.auto_pull if auto_pull is None else auto_pull
    # TODO remove version field
    # TODO move parent field outside of object
    dataset_version = DatasetVersion(
        version=num_versions + 1,
        parent=workspace_ds.parent,
        collection=collection_obj.oid,
        transform_mapping_id=transform_mapping.oid,
        tags=workspace_ds.tags.copy(),
        commit_info=CommitInfo(
            created_by=username,
            commit_time=curr_time,
            commit_message=message,
        ),
        auto_pull=auto_pull,
    )
    dataset_version.digest()
    client.db.add_dataset_version(dataset_version)
    client.db.add_dataset_assignment(dataset_name, dataset_version)
    client.db.write_all()

    workspace_ds.staged_time = curr_time
    workspace_ds.parent = dataset_version.oid
    workspace_ds.dataset_name = dataset_name
    workspace_ds.auto_pull = auto_pull
    write_data_file(
        workspace_path / WorkspacePath.DATASET,
        json_dumps(workspace_ds.format()).encode(),
        overwrite_existing=True,
    )
    dataset_identifier = format_dataset_identifier(
        dataset_name,
        dataset_version.version,
    )
    print(f"Committed {dataset_identifier}")

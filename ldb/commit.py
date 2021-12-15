import getpass
import os
from pathlib import Path

from ldb.dataset import CommitInfo, Dataset, DatasetVersion
from ldb.path import InstanceDir, WorkspacePath
from ldb.utils import (
    current_time,
    format_dataset_identifier,
    get_hash_path,
    hash_data,
    json_dumps,
    load_data_file,
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
    message: str,
) -> None:
    workspace_path = Path(os.path.normpath(workspace_path))
    workspace_ds = load_workspace_dataset(workspace_path)
    dataset_name = workspace_ds.dataset_name
    if workspace_dataset_is_clean(
        ldb_dir,
        workspace_ds,
        workspace_path,
    ):
        print("Nothing to commit.")
        return
    collection_obj = collection_dir_to_object(
        workspace_path / WorkspacePath.COLLECTION,
    )
    collection_obj_bytes = json_dumps(collection_obj).encode()
    collection_hash = hash_data(collection_obj_bytes)
    collection_path = get_hash_path(
        ldb_dir / InstanceDir.COLLECTIONS,
        collection_hash,
    )
    write_data_file(
        collection_path,
        collection_obj_bytes,
        overwrite_existing=False,
    )

    curr_time = current_time()
    username = getpass.getuser()

    dataset_file_path = ldb_dir / InstanceDir.DATASETS / dataset_name
    try:
        dataset = Dataset.parse(load_data_file(dataset_file_path))
    except FileNotFoundError:
        dataset = Dataset(
            name=dataset_name,
            created_by=username,
            created=curr_time,
            versions=[],
        )
    dataset_version = DatasetVersion(
        version=len(dataset.versions) + 1,
        parent=workspace_ds.parent,
        collection=collection_hash,
        tags=workspace_ds.tags.copy(),
        commit_info=CommitInfo(
            created_by=username,
            commit_time=curr_time,
            commit_message=message,
        ),
    )
    dataset_version_bytes = json_dumps(dataset_version.format()).encode()
    dataset_version_hash = hash_data(dataset_version_bytes)
    dataset_version_file_path = get_hash_path(
        ldb_dir / InstanceDir.DATASET_VERSIONS,
        dataset_version_hash,
    )
    write_data_file(dataset_version_file_path, dataset_version_bytes)
    dataset.versions.append(dataset_version_hash)
    write_data_file(
        dataset_file_path,
        json_dumps(dataset.format()).encode(),
        overwrite_existing=True,
    )
    workspace_ds.staged_time = curr_time
    workspace_ds.parent = dataset_version_hash
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

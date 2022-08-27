import getpass
import os
from pathlib import Path
from typing import Optional

from ldb.dataset import (
    CommitInfo,
    Dataset,
    DatasetVersion,
    ensure_all_collection_dir_keys_contained,
)
from ldb.db.collection import CollectionDB
from ldb.db.dataset import DatasetDB
from ldb.db.dataset_version import DatasetVersionDB
from ldb.exceptions import DatasetNotFoundError
from ldb.path import InstanceDir, WorkspacePath
from ldb.transform import save_transform_object
from ldb.utils import (
    DATASET_PREFIX,
    current_time,
    format_dataset_identifier,
    json_dumps,
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
        ldb_dir,
        workspace_ds,
        workspace_path,
    ):
        print("Nothing to commit.")
        return
    ensure_all_collection_dir_keys_contained(
        workspace_path / WorkspacePath.COLLECTION,
        ldb_dir / InstanceDir.DATA_OBJECT_INFO,
    )
    collection_obj = collection_dir_to_object(
        workspace_path / WorkspacePath.COLLECTION,
    )
    collection_obj.digest()
    CollectionDB.from_ldb_dir(ldb_dir).add_obj(collection_obj)
    transform_hash = save_transform_object(ldb_dir, workspace_path)

    curr_time = current_time()
    username = getpass.getuser()

    dataset_db = DatasetDB.from_ldb_dir(ldb_dir)
    try:
        dataset = dataset_db.get_obj(dataset_name)
    except DatasetNotFoundError:
        dataset = Dataset(
            name=dataset_name,
            created_by=username,
            created=curr_time,
            versions=[],
        )
    auto_pull = workspace_ds.auto_pull if auto_pull is None else auto_pull

    dataset_version = DatasetVersion(
        version=len(dataset.versions) + 1,
        parent=workspace_ds.parent,
        collection=collection_obj.oid,
        transform_mapping_id=transform_hash,
        tags=workspace_ds.tags.copy(),
        commit_info=CommitInfo(
            created_by=username,
            commit_time=curr_time,
            commit_message=message,
        ),
        auto_pull=auto_pull,
    )
    dataset_version.digest()
    dataset.versions.append(dataset_version.oid)
    dataset.digest()

    DatasetVersionDB.from_ldb_dir(ldb_dir).add_obj(
        dataset_version,
    )
    dataset_db.add_obj(dataset)

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

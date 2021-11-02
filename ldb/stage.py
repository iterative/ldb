import json
import os
import shutil
from pathlib import Path

from ldb.dataset import Dataset, DatasetVersion, workspace_dataset_is_clean
from ldb.exceptions import LDBException
from ldb.path import InstanceDir, WorkspacePath
from ldb.utils import (
    current_time,
    format_dataset_identifier,
    format_datetime,
    get_hash_path,
    load_data_file,
    parse_dataset_identifier,
    write_data_file,
)


def stage(
    ldb_dir: Path,
    dataset_identifier: str,
    workspace_path: Path,
    force: bool = False,
) -> None:
    workspace_path = Path(os.path.normpath(workspace_path))
    if workspace_path.is_dir():
        workspace_ds_path = workspace_path / WorkspacePath.DATASET
        curr_workspace_ds_obj = (
            load_data_file(workspace_ds_path)
            if workspace_ds_path.is_file()
            else None
        )
        if (
            not force
            and curr_workspace_ds_obj is not None
            and not workspace_dataset_is_clean(
                ldb_dir,
                curr_workspace_ds_obj,
                workspace_path,
            )
        ):
            raise LDBException(
                "Uncommitted dataset in workspace: "
                f"{repr(os.fspath(workspace_path))}\n"
                "Use the --force option to delete changes and overwrite",
            )
        for path in workspace_path.iterdir():
            if path.name != WorkspacePath.BASE.name or not path.is_dir():
                if force:
                    if path.is_dir():
                        shutil.rmtree(path)
                    else:
                        path.unlink()
                else:
                    raise LDBException(
                        "Workspace is not empty: "
                        f"{repr(os.fspath(workspace_path))}\n"
                        "Use the --force option to delete workspace contents",
                    )
    ds_name, ds_version_num = parse_dataset_identifier(dataset_identifier)
    workspace_ds_obj = {
        "dataset_name": ds_name,
        "staged_time": format_datetime(current_time()),
        "parent": None,
        "tags": [],
    }
    try:
        dataset_obj = Dataset.parse(
            load_data_file(ldb_dir / InstanceDir.DATASETS / ds_name),
        )
    except FileNotFoundError as exc:
        if ds_version_num is not None:
            ds_ident = format_dataset_identifier(ds_name)
            raise LDBException(
                f"{dataset_identifier} does not exist\n"
                f"To stage a new dataset, use {ds_ident}",
            ) from exc
        collection_obj = None
        message = (
            f"Staged new dataset {dataset_identifier} "
            f"at {repr(os.fspath(workspace_path))}"
        )
    else:
        if ds_version_num is None:
            dataset_version_hash = dataset_obj.versions[-1]
            ds_version_num = len(dataset_obj.versions)
        else:
            try:
                dataset_version_hash = dataset_obj.versions[ds_version_num - 1]
            except IndexError as exc:
                latest_dataset = format_dataset_identifier(
                    ds_name,
                    len(dataset_obj.versions),
                )
                raise LDBException(
                    f"{dataset_identifier} does not exist\n"
                    f"The latest version is {latest_dataset}",
                ) from exc
        dataset_version_obj = DatasetVersion.parse(
            load_data_file(
                get_hash_path(
                    ldb_dir / InstanceDir.DATASET_VERSIONS,
                    dataset_version_hash,
                ),
            ),
        )
        workspace_ds_obj["parent"] = dataset_version_hash
        workspace_ds_obj["tags"] = dataset_version_obj.tags.copy()
        collection_obj = load_data_file(
            get_hash_path(
                ldb_dir / InstanceDir.COLLECTIONS,
                dataset_version_obj.collection,
            ),
        )
        curr_dataset_ident = format_dataset_identifier(
            ds_name,
            ds_version_num,
        )
        message = (
            f"Staged {curr_dataset_ident} "
            f"at {repr(os.fspath(workspace_path))}"
        )
    stage_workspace(workspace_path, workspace_ds_obj, collection_obj)
    print(message)


def stage_workspace(
    workspace_path: Path,
    workspace_ds_obj,
    collection_obj=None,
) -> None:
    collection_path = workspace_path / WorkspacePath.COLLECTION
    workspace_ds_bytes = json.dumps(workspace_ds_obj).encode()
    if collection_obj:
        collection_path_data = get_workspace_collection_path_data(
            collection_path,
            collection_obj,
        )
        collection_path.mkdir(parents=True, exist_ok=True)
        write_workspace_collection(collection_path, collection_path_data)
    else:
        if collection_path.exists():
            shutil.rmtree(collection_path)
        collection_path.mkdir(parents=True)
    write_data_file(
        workspace_path / WorkspacePath.DATASET,
        workspace_ds_bytes,
    )


def get_workspace_collection_path_data(path, collection_obj):
    return [
        (get_hash_path(path, data_object_hash), annotation_hash or "")
        for data_object_hash, annotation_hash in collection_obj.items()
    ]


def write_workspace_collection(collection_path, path_data):
    for path, data in path_data:
        path.parent.mkdir(exist_ok=True)
        with path.open("w") as file:
            file.write(data)
    path_set = {d[0] for d in path_data}
    for path in collection_path.glob("*/*"):
        if path not in path_set:
            path.unlink()
    path_parent_set = {p.parent for p in path_set}
    for path in collection_path.glob("*"):
        if path not in path_parent_set:
            path.rmdir()

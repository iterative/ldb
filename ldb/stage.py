import os
import shutil
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from ldb.config import get_ldb_dir
from ldb.core import init_quickstart
from ldb.dataset import Dataset, DatasetVersion
from ldb.exceptions import LDBException
from ldb.path import InstanceDir, WorkspacePath
from ldb.pull import get_collection_with_updated_annotations
from ldb.utils import (
    DATASET_PREFIX,
    ROOT,
    current_time,
    format_dataset_identifier,
    get_hash_path,
    json_dumps,
    load_data_file,
    make_target_dir,
    parse_dataset_identifier,
    write_data_file,
)
from ldb.workspace import (
    WorkspaceDataset,
    ensure_path_is_empty_workspace,
    workspace_dataset_is_clean,
)


def stage(
    dataset_identifier: str,
    workspace_path: Path,
    force: bool = False,
    make_parent_dirs: bool = False,
    ldb_dir: Optional[Path] = None,
) -> None:
    if ldb_dir is None:
        ldb_dir = get_ldb_dir()
    if not ldb_dir.is_dir():
        ldb_dir = init_quickstart()
    stage_with_instance(
        ldb_dir,
        dataset_identifier,
        workspace_path,
        force=force,
        make_parent_dirs=make_parent_dirs,
    )


def stage_new(
    workspace_path: Path,
    ds_name: str,
    make_parent_dirs: bool = False,
) -> None:
    make_target_dir(workspace_path, parents=make_parent_dirs)
    workspace_ds_obj = WorkspaceDataset(
        dataset_name=ds_name,
        staged_time=current_time(),
        parent="",
        tags=[],
    )
    stage_workspace(workspace_path, workspace_ds_obj, None)
    print(f"Staged {DATASET_PREFIX}{ds_name} at {os.fspath(workspace_path)!r}")


def stage_with_instance(  # pylint: disable=too-many-statements
    ldb_dir: Path,
    dataset_identifier: str,
    workspace_path: Path,
    force: bool = False,
    make_parent_dirs: bool = False,
) -> None:
    ds_name, ds_version_num = parse_dataset_identifier(dataset_identifier)
    if ds_name == ROOT:
        raise ValueError(
            f"Cannot stage dataset named {DATASET_PREFIX}{ds_name}",
        )
    workspace_path = Path(os.path.normpath(workspace_path))
    created_new_dir = make_target_dir(
        workspace_path,
        parents=make_parent_dirs,
    )
    if not created_new_dir:
        if not force:
            try:
                curr_workspace_ds_obj = WorkspaceDataset.parse(
                    load_data_file(
                        workspace_path / WorkspacePath.DATASET,
                    ),
                )
            except FileNotFoundError:
                pass
            else:
                if not workspace_dataset_is_clean(
                    ldb_dir,
                    curr_workspace_ds_obj,
                    workspace_path,
                ):
                    raise LDBException(
                        "Unsaved changes to workspace dataset.\n"
                        "Commit changes or use the --force option to "
                        "overwrite them.",
                    )
        ensure_path_is_empty_workspace(workspace_path, force)
    workspace_ds_obj = WorkspaceDataset(
        dataset_name=ds_name,
        staged_time=current_time(),
        parent="",
        tags=[],
        auto_pull=False,
    )
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
        transform_obj = None
        message = (
            f"Staged new dataset {dataset_identifier} "
            f"at {os.fspath(workspace_path)!r}"
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
        workspace_ds_obj.parent = dataset_version_hash
        workspace_ds_obj.tags = dataset_version_obj.tags.copy()
        workspace_ds_obj.auto_pull = dataset_version_obj.auto_pull
        collection_obj = load_data_file(
            get_hash_path(
                ldb_dir / InstanceDir.COLLECTIONS,
                dataset_version_obj.collection,
            ),
        )
        if workspace_ds_obj.auto_pull:
            collection_obj = dict(
                get_collection_with_updated_annotations(
                    ldb_dir,
                    collection_obj.keys(),
                )[0],
            )
        transform_obj = load_data_file(
            get_hash_path(
                ldb_dir / InstanceDir.TRANSFORM_MAPPINGS,
                dataset_version_obj.transform_mapping_id,
            ),
        )
        curr_dataset_ident = format_dataset_identifier(
            ds_name,
            ds_version_num,
        )
        message = (
            f"Staged {curr_dataset_ident} at {os.fspath(workspace_path)!r}"
        )
    stage_workspace(
        workspace_path,
        workspace_ds_obj,
        collection_obj,
        transform_obj,
    )
    print(message)


def stage_workspace(
    workspace_path: Path,
    workspace_ds_obj: WorkspaceDataset,
    collection_obj: Optional[Dict[str, Optional[str]]] = None,
    transform_obj: Optional[Dict[str, Sequence[str]]] = None,
) -> None:
    collection_path = workspace_path / WorkspacePath.COLLECTION
    workspace_ds_bytes = json_dumps(workspace_ds_obj.format()).encode()
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

    transform_path = workspace_path / WorkspacePath.TRANSFORM_MAPPING
    if transform_obj:
        transform_path_data = transform_obj_to_path_items(
            transform_path,
            transform_obj,
        )

        transform_path.mkdir(parents=True, exist_ok=True)
        write_workspace_collection(transform_path, transform_path_data)
    else:
        if transform_path.exists():
            shutil.rmtree(transform_path)
        transform_path.mkdir(parents=True)

    write_data_file(
        workspace_path / WorkspacePath.DATASET,
        workspace_ds_bytes,
    )


def transform_obj_to_path_items(
    transform_path: Path,
    transform_obj: Mapping[str, Sequence[str]],
) -> List[Tuple[Path, str]]:
    return [
        (
            get_hash_path(transform_path, data_object_hash),
            json_dumps(transform_ids),
        )
        for data_object_hash, transform_ids in transform_obj.items()
    ]


def get_workspace_collection_path_data(
    path: Path,
    collection_obj: Dict[str, Optional[str]],
) -> List[Tuple[Path, str]]:
    return [
        (get_hash_path(path, data_object_hash), annotation_hash or "")
        for data_object_hash, annotation_hash in collection_obj.items()
    ]


def write_workspace_collection(
    collection_path: Path,
    path_data: List[Tuple[Path, str]],
) -> None:
    for path, data in path_data:
        path.parent.mkdir(exist_ok=True)
        path.write_text(data)
    path_set = {d[0] for d in path_data}
    for path in collection_path.glob("*/*"):
        if path not in path_set:
            path.unlink()
    path_parent_set = {p.parent for p in path_set}
    for path in collection_path.glob("*"):
        if path not in path_parent_set:
            path.rmdir()

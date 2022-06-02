import os
from pathlib import Path

import pytest
from tomlkit import document
from tomlkit.toml_document import TOMLDocument

from ldb import config
from ldb.config import ConfigType
from ldb.core import get_ldb_instance
from ldb.dataset import Dataset, get_collection_dir_items
from ldb.exceptions import WorkspaceError
from ldb.main import main
from ldb.path import InstanceDir, WorkspacePath
from ldb.stage import stage_with_instance
from ldb.transform import get_transform_mapping_dir_items
from ldb.utils import DATASET_PREFIX, current_time, load_data_file
from ldb.workspace import WorkspaceDataset


def is_workspace(dir_path: Path) -> bool:
    return (
        (dir_path / WorkspacePath.BASE).is_dir()
        and (dir_path / WorkspacePath.COLLECTION).is_dir()
        and (dir_path / WorkspacePath.TRANSFORM_MAPPING).is_dir()
        and (dir_path / WorkspacePath.DATASET).is_file()
    )


def test_stage_cli_new_dataset(tmp_path, global_base):
    workspace_path = tmp_path / "workspace"
    ds_name = "my-new-dataset"
    ret = main(
        ["stage", f"ds:{ds_name}", "-t", f"{os.fspath(workspace_path)}"],
    )

    # TODO: mock current_time instead of setting staged_time
    curr_time = current_time()
    workspace_ds = WorkspaceDataset.parse(
        load_data_file(workspace_path / WorkspacePath.DATASET),
    )
    workspace_ds.staged_time = curr_time
    expected_workspace_ds = WorkspaceDataset(
        dataset_name=ds_name,
        staged_time=curr_time,
        parent="",
        tags=[],
    )
    cfg: TOMLDocument = config.load_first([ConfigType.INSTANCE]) or document()
    assert ret == 0
    assert is_workspace(workspace_path)
    assert workspace_ds == expected_workspace_ds
    assert cfg["core"]["read_any_cloud_location"]  # type: ignore[index]
    assert cfg["core"]["auto_index"]  # type: ignore[index]


def test_stage_cli_existing_dataset(
    tmp_path,
    global_base,
    staged_ds_fashion_with_transforms,
):
    ldb_dir = get_ldb_instance()
    main(["commit"])
    workspace_path = tmp_path / "workspace2"
    ds_name = staged_ds_fashion_with_transforms[len(DATASET_PREFIX) :]
    ret = main(
        ["stage", f"ds:{ds_name}", "-t", f"{os.fspath(workspace_path)}"],
    )

    curr_time = current_time()
    workspace_ds = WorkspaceDataset.parse(
        load_data_file(workspace_path / WorkspacePath.DATASET),
    )
    dataset_obj = Dataset.parse(
        load_data_file(ldb_dir / InstanceDir.DATASETS / ds_name),
    )
    workspace_ds.staged_time = curr_time
    collection_items = dict(
        get_collection_dir_items(workspace_path / WorkspacePath.COLLECTION),
    )
    transform_items = dict(
        get_transform_mapping_dir_items(
            workspace_path / WorkspacePath.TRANSFORM_MAPPING,
        ),
    )
    expected_workspace_ds = WorkspaceDataset(
        dataset_name=ds_name,
        staged_time=curr_time,
        parent=dataset_obj.versions[-1],
        tags=[],
    )
    assert ret == 0
    assert is_workspace(workspace_path)
    assert workspace_ds == expected_workspace_ds
    assert len(collection_items) == 32
    assert len(list(filter(None, collection_items.values()))) == 23
    assert len(transform_items) == 12


@pytest.mark.parametrize(
    "stage_before",
    [True, False],
)
def test_stage_cli_populated_directory(stage_before, tmp_path, global_base):
    ds_name = "my-new-dataset"
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    if stage_before:
        ret_before = main(
            ["stage", f"ds:{ds_name}", "-t", f"{os.fspath(workspace_path)}"],
        )
    else:
        ret_before = 0
    (workspace_path / "file.txt").touch()
    ret = main(
        ["stage", f"ds:{ds_name}", "-t", f"{os.fspath(workspace_path)}"],
    )
    assert ret_before == 0
    assert ret == 1
    assert is_workspace(workspace_path) == stage_before


def test_stage_cli_existing_empty_directory(tmp_path, global_base):
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    ds_name = "my-new-dataset"
    ret = main(
        ["stage", f"ds:{ds_name}", "-t", f"{os.fspath(workspace_path)}"],
    )
    assert ret == 0
    assert is_workspace(workspace_path)


@pytest.mark.parametrize(
    "stage_before",
    [True, False],
)
def test_stage_populated_directory(stage_before, tmp_path, ldb_instance):
    ds_name = "my-new-dataset"
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    if stage_before:
        stage_with_instance(ldb_instance, f"ds:{ds_name}", workspace_path)
        error_match = "Workspace is not empty"
    else:
        error_match = "Not a workspace or an empty directory"
    (workspace_path / "file.txt").touch()
    with pytest.raises(WorkspaceError, match=error_match):
        stage_with_instance(ldb_instance, f"ds:{ds_name}", workspace_path)


@pytest.mark.parametrize("ds_name", ["root", "a:b"])
def test_stage_invalid_dataset_name(tmp_path, ldb_instance, ds_name):
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    with pytest.raises(ValueError):
        stage_with_instance(ldb_instance, f"ds:{ds_name}", workspace_path)

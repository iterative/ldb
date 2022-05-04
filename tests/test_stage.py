import os
from pathlib import Path

import pytest
from tomlkit import document
from tomlkit.toml_document import TOMLDocument

from ldb import config
from ldb.config import ConfigType
from ldb.exceptions import WorkspaceError
from ldb.main import main
from ldb.path import WorkspacePath
from ldb.stage import stage
from ldb.utils import current_time, load_data_file
from ldb.workspace import WorkspaceDataset


def is_workspace(dir_path: Path) -> bool:
    return (
        (dir_path / WorkspacePath.BASE).is_dir()
        and (dir_path / WorkspacePath.COLLECTION).is_dir()
        and (dir_path / WorkspacePath.DATASET).is_file()
    )


def test_stage_cli_new_dataset(tmp_path, global_base):
    workspace_path = tmp_path / "workspace"
    ds_name = "my-new-dataset"
    ret = main(
        ["stage", f"ds:{ds_name}", "-t", f"{os.fspath(workspace_path)}"],
    )

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
        stage(ldb_instance, f"ds:{ds_name}", workspace_path)
        error_match = "Workspace is not empty"
    else:
        error_match = "Not a workspace or an empty directory"
    (workspace_path / "file.txt").touch()
    with pytest.raises(WorkspaceError, match=error_match):
        stage(ldb_instance, f"ds:{ds_name}", workspace_path)


@pytest.mark.parametrize("ds_name", ["root", "a:b"])
def test_stage_invalid_dataset_name(tmp_path, ldb_instance, ds_name):
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    with pytest.raises(ValueError):
        stage(ldb_instance, f"ds:{ds_name}", workspace_path)

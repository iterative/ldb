import os
from pathlib import Path

import pytest

from ldb.exceptions import WorkspaceError
from ldb.main import main
from ldb.path import WorkspacePath
from ldb.stage import stage
from ldb.utils import current_time, load_data_file
from ldb.workspace import WorkspaceDataset


def is_workspace(dir_path: Path):
    return (
        (dir_path / WorkspacePath.BASE).is_dir()
        and (dir_path / WorkspacePath.COLLECTION).is_dir()
        and (dir_path / WorkspacePath.DATASET).is_file()
    )


def test_stage_cli_new_dataset(tmp_path, global_base):
    workspace_path = tmp_path / "workspace"
    ds_name = "my-new-dataset"
    ret = main(["stage", f"ds:{ds_name}", f"{os.fspath(workspace_path)}"])

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
    assert ret == 0
    assert is_workspace(workspace_path)
    assert workspace_ds == expected_workspace_ds


def test_stage_cli_populated_directory(tmp_path, global_base):
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "file.txt").touch()
    ds_name = "my-new-dataset"
    ret = main(["stage", f"ds:{ds_name}", f"{os.fspath(workspace_path)}"])
    assert ret == 1
    assert not is_workspace(workspace_path)


def test_stage_cli_existing_empty_directory(tmp_path, global_base):
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    ds_name = "my-new-dataset"
    ret = main(["stage", f"ds:{ds_name}", f"{os.fspath(workspace_path)}"])
    assert ret == 0
    assert is_workspace(workspace_path)


def test_stage_populated_directory(tmp_path, ldb_instance):
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "file.txt").touch()
    ds_name = "my-new-dataset"
    with pytest.raises(WorkspaceError, match="Workspace is not empty"):
        stage(ldb_instance, f"ds:{ds_name}", workspace_path)

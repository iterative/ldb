import os
from pathlib import Path

from ldb.main import main
from ldb.path import WorkspacePath
from ldb.utils import load_data_file

WORKSPACE_DATASET_KEYS = (
    "dataset_name",
    "staged_time",
    "parent",
    "tags",
)


def is_workspace(dir_path: Path):
    return (
        (dir_path / WorkspacePath.BASE).is_dir()
        and (dir_path / WorkspacePath.COLLECTION).is_dir()
        and (dir_path / WorkspacePath.DATASET).is_file()
    )


def test_stage_new_dataset(tmp_path, global_base):
    workspace_path = tmp_path / "workspace"
    ds_name = "my-new-dataset"
    ret = main(["stage", f"ds:{ds_name}", f"{os.fspath(workspace_path)}"])
    assert ret == 0
    assert is_workspace(workspace_path)
    assert (
        tuple(load_data_file(workspace_path / WorkspacePath.DATASET))
        == WORKSPACE_DATASET_KEYS
    )


def test_stage_populated_directory(tmp_path, global_base):
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "file.txt").touch()
    ds_name = "my-new-dataset"
    ret = main(["stage", f"ds:{ds_name}", f"{os.fspath(workspace_path)}"])
    assert ret == 1
    assert not is_workspace(workspace_path)

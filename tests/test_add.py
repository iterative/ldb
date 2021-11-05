import os
from pathlib import Path
from typing import Iterable, List

from ldb.main import main
from ldb.path import WorkspacePath
from ldb.stage import stage_workspace
from ldb.utils import current_time
from ldb.workspace import WorkspaceDataset


def get_staged_object_file_paths(workspace_path: Path) -> List[Path]:
    return list((workspace_path / WorkspacePath.COLLECTION).glob("*/*"))


def num_empty_files(paths: Iterable[Path]) -> int:
    num = 0
    for path in paths:
        num += bool(path.read_text())
    return num


def test_add_storage_location(tmp_path, data_dir, ldb_instance):
    workspace_path = tmp_path / "workspace"
    dir_to_add = data_dir / "fashion-mnist/original"
    stage_workspace(
        workspace_path,
        WorkspaceDataset(
            dataset_name="my-new-dataset",
            staged_time=current_time(),
            parent="",
            tags=[],
        ),
    )
    os.chdir(workspace_path)
    ret = main(["add", f"{os.fspath(dir_to_add)}"])
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 32
    assert num_empty_files(object_file_paths) == 23

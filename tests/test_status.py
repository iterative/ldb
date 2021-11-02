import os

from ldb.main import main
from ldb.stage import stage_workspace
from ldb.status import WorkspaceStatus, status
from ldb.utils import current_time, format_datetime


def test_add_storage_location(tmp_path, data_dir, ldb_instance):
    workspace_path = tmp_path / "workspace"
    dir_to_add = data_dir / "fashion-mnist/original"
    stage_workspace(
        workspace_path,
        {
            "dataset_name": "my-dataset",
            "staged_time": format_datetime(current_time()),
            "parent": None,
            "tags": [],
        },
    )
    os.chdir(workspace_path)
    main(["add", f"{os.fspath(dir_to_add)}"])
    ws_status = status(workspace_path)
    expected_ws_status = WorkspaceStatus(
        dataset_name="my-dataset",
        num_data_objects=32,
        num_annotations=23,
    )
    assert ws_status == expected_ws_status

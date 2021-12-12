import os

from ldb.main import main
from ldb.status import WorkspaceStatus, status


def test_add_storage_location(data_dir, ldb_instance, workspace_path):
    dir_to_add = os.fspath(data_dir / "fashion-mnist/original")
    main(["index", "-f", "bare", dir_to_add])
    main(["add", dir_to_add])
    ws_status = status(ldb_instance, workspace_path)
    expected_ws_status = WorkspaceStatus(
        dataset_name="my-dataset",
        num_data_objects=32,
        num_annotations=23,
    )
    assert ws_status == expected_ws_status

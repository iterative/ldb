import os

from ldb.main import main
from ldb.status import WorkspaceStatus, status
from ldb.utils import chdir


def test_status_added_storage_location(
    data_dir,
    fashion_mnist_session,
    global_workspace_path,
):
    ldb_instance = fashion_mnist_session
    dir_to_add = os.fspath(data_dir / "fashion-mnist/original")
    with chdir(global_workspace_path):
        main(["add", dir_to_add])
        ws_status = status(ldb_instance, "")
    expected_ws_status = WorkspaceStatus(
        dataset_name="my-dataset",
        dataset_version=0,
        num_data_objects=32,
        num_annotations=23,
    )
    assert ws_status == expected_ws_status

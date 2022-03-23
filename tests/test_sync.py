from ldb.main import main
from ldb.utils import DATASET_PREFIX, ROOT

from .add import AddCommandBase
from .utils import get_staged_object_file_paths, num_empty_files


class TestSync(AddCommandBase):
    COMMAND = "sync"


def test_sync_with_add_and_del(workspace_path, index_original):
    main(["add", f"{DATASET_PREFIX}{ROOT}"])
    add_ret = main(
        [
            "add",
            "0x3c679fd1b8537dc7da1272a085e388e6",
            "0x982814b9116dce7882dfc31636c3ff7a",
            "0x232bab540dbfbd2fccae2e57e684663e",
            "0x95789bb1ac140460cefc97a6e66a9ee8",
            "0xd830d9f128e04678499e1fc52e935c4a",
        ],
    )
    ret = main(
        [
            "sync",
            "0x3c679fd1b8537dc7da1272a085e388e6",
            "0x982814b9116dce7882dfc31636c3ff7a",
            "0xebbc6c0cebb66738942ee56513f9ee2f",
            "0x1e0759182b328fd22fcdb5e6beb54adf",
        ],
    )
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert add_ret == 0
    assert ret == 0
    assert len(object_file_paths) == 4
    assert num_empty_files(object_file_paths) == 2


def test_sync_empty_workspace(workspace_path, index_original):
    main(["add", f"{DATASET_PREFIX}{ROOT}"])
    ret = main(["sync"])
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 0
    assert num_empty_files(object_file_paths) == 0

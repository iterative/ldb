import pytest

from ldb.main import main
from ldb.utils import DATASET_PREFIX, ROOT

from .add import AddCommandBase
from .utils import (
    get_staged_object_file_paths,
    get_workspace_counts,
    num_empty_files,
)


class TestSync(AddCommandBase):
    COMMAND = "sync"


class TestSyncPhysical(AddCommandBase):
    COMMAND = "sync"
    PHYSICAL = True


@pytest.mark.parametrize("physical", [True, False])
def test_sync_with_add_and_del(workspace_path, index_original, physical):
    main(["add", f"{DATASET_PREFIX}{ROOT}"])
    add_ret = main(
        [
            "add",
            "id:3c679fd1b8537dc7da1272a085e388e6",
            "id:982814b9116dce7882dfc31636c3ff7a",
            "id:232bab540dbfbd2fccae2e57e684663e",
            "id:95789bb1ac140460cefc97a6e66a9ee8",
            "id:d830d9f128e04678499e1fc52e935c4a",
        ],
    )
    cmd_args = [
        "sync",
        "id:3c679fd1b8537dc7da1272a085e388e6",
        "id:982814b9116dce7882dfc31636c3ff7a",
        "id:ebbc6c0cebb66738942ee56513f9ee2f",
        "id:1e0759182b328fd22fcdb5e6beb54adf",
    ]
    if physical:
        cmd_args.append("--physical")
    ret = main(cmd_args)
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert add_ret == 0
    assert ret == 0
    assert len(object_file_paths) == 4
    assert num_empty_files(object_file_paths) == 2
    if physical:
        assert get_workspace_counts(workspace_path) == (4, 2)
    else:
        assert get_workspace_counts(workspace_path) == (0, 0)


@pytest.mark.parametrize(
    "args",
    [
        ["sync"],
        ["sync", "--physical"],
        ["sync", "--logical"],
    ],
    ids=["empty-normal", "empty-physical", "empty-logical"],
)
def test_sync_empty_workspace(workspace_path, index_original, args):
    main(["add", f"{DATASET_PREFIX}{ROOT}"])
    ret = main(args)
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 0
    assert num_empty_files(object_file_paths) == 0

import os
from pathlib import Path
from typing import Iterable, List

from ldb.main import main
from ldb.path import WorkspacePath


def get_staged_object_file_paths(workspace_path: Path) -> List[Path]:
    return list((workspace_path / WorkspacePath.COLLECTION).glob("*/*"))


def num_empty_files(paths: Iterable[Path]) -> int:
    num = 0
    for path in paths:
        num += bool(path.read_text())
    return num


def test_add_storage_location(workspace_path, data_dir):
    dir_to_add = data_dir / "fashion-mnist/original"
    ret = main(["add", f"{os.fspath(dir_to_add)}"])
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 32
    assert num_empty_files(object_file_paths) == 23


def test_add_data_objects(workspace_path, data_dir):
    dir_to_add = data_dir / "fashion-mnist/original"
    ret = main(["index", f"{os.fspath(dir_to_add)}"])
    ret = main(
        [
            "add",
            "0x3c679fd1b8537dc7da1272a085e388e6",
            "0x982814b9116dce7882dfc31636c3ff7a",
            "0xebbc6c0cebb66738942ee56513f9ee2f",
            "0x1e0759182b328fd22fcdb5e6beb54adf",
        ],
    )
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 4
    assert num_empty_files(object_file_paths) == 2


def test_add_datasets(workspace_path, data_dir):
    dir_to_add = data_dir / "fashion-mnist/original"
    main(["index", f"{os.fspath(dir_to_add)}"])
    main(["stage", "ds:a"])
    main(
        [
            "add",
            "0x3c679fd1b8537dc7da1272a085e388e6",
            "0x982814b9116dce7882dfc31636c3ff7a",
            "0xebbc6c0cebb66738942ee56513f9ee2f",
            "0x1e0759182b328fd22fcdb5e6beb54adf",
        ],
    )
    main(["commit"])
    main(["stage", "ds:b"])
    main(
        [
            "add",
            "0x982814b9116dce7882dfc31636c3ff7a",
            "0x1e0759182b328fd22fcdb5e6beb54adf",
            "0x2f3533f1e35349602fbfaf0ec9b3ef3f",
            "0x95789bb1ac140460cefc97a6e66a9ee8",
            "0xe1c3ef93e4e1cf108fa2a4c9d6e03af2",
        ],
    )
    main(["commit"])
    main(["stage", "ds:c"])
    ret = main(["add", "ds:a", "ds:b"])
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 7
    assert num_empty_files(object_file_paths) == 3

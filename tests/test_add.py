import os
import shutil

import pytest

from ldb.core import add_default_read_add_storage
from ldb.main import main
from ldb.utils import DATASET_PREFIX, ROOT

from .utils import (
    DATA_DIR,
    get_staged_object_file_paths,
    num_empty_files,
    stage_new_workspace,
)


def test_add_storage_location(workspace_path, data_dir):
    dir_to_add = data_dir / "fashion-mnist/original"
    ret = main(["add", f"{os.fspath(dir_to_add)}"])
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 32
    assert num_empty_files(object_file_paths) == 23


def test_add_data_objects(workspace_path, index_original):
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


def test_add_datasets(workspace_path, ds_a, ds_b):
    main(["stage", f"{DATASET_PREFIX}c"])
    ret = main(["add", ds_a, ds_b])
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 7
    assert num_empty_files(object_file_paths) == 3


@pytest.mark.parametrize(
    "add_args,expected",
    [
        ([], (32, 23)),
        (["--file", "@"], (32, 23)),
        (["--query", "@"], (23, 23)),
        (["--file", "fs.size > `400`"], (26, 20)),
        (
            [
                "--query",
                "inference.label != `null` || inference.label == label",
            ],
            (13, 4),
        ),
        (
            [
                "--file",
                "fs.size > `400`",
                "--query",
                "inference.label != `null` || inference.label == label",
            ],
            (9, 3),
        ),
    ],
)
def test_add_root_dataset(workspace_path, add_args, expected):
    main(["index", os.fspath(DATA_DIR / "fashion-mnist/original")])
    main(["index", os.fspath(DATA_DIR / "fashion-mnist/updates")])
    ret = main(["add", f"{DATASET_PREFIX}{ROOT}", *add_args])
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert (
        len(object_file_paths),
        num_empty_files(object_file_paths),
    ) == expected


def test_add_root_dataset_query(workspace_path, index_original):
    ret = main(
        [
            "add",
            f"{DATASET_PREFIX}{ROOT}",
            "--query",
            "label != `null` && label > `2` && label < `8`",
        ],
    )
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 14
    assert num_empty_files(object_file_paths) == 14


def test_add_current_workspace(workspace_path, data_dir, ldb_instance):
    add_default_read_add_storage(ldb_instance)
    shutil.copytree(
        data_dir / "fashion-mnist/original/has_both/train",
        "./train",
    )
    ret = main(["add", "."])
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 13
    assert num_empty_files(object_file_paths) == 13


def test_add_another_workspace(
    workspace_path,
    data_dir,
    ldb_instance,
    tmp_path,
):
    other_workspace_path = tmp_path / "other-workspace"
    stage_new_workspace(other_workspace_path)
    os.chdir(other_workspace_path)
    main(
        ["add", os.fspath(data_dir / "fashion-mnist/original/has_both/train")],
    )
    os.chdir(workspace_path)
    ret = main(["add", os.fspath(other_workspace_path)])

    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 13
    assert num_empty_files(object_file_paths) == 13

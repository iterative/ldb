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


@pytest.mark.parametrize(
    "index_first,objects,annots",
    [
        (True, 32, 23),
        (False, 23, 23),
    ],
)
def test_add_storage_location(index_first, objects, annots, workspace_path):
    dir_to_add = os.fspath(DATA_DIR / "fashion-mnist/original")
    if index_first:
        ret = main(["index", "-m", "bare", dir_to_add])
    ret = main(["add", dir_to_add])
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == objects
    assert num_empty_files(object_file_paths) == annots


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
    for path_obj in (
        DATA_DIR / "fashion-mnist/original",
        DATA_DIR / "fashion-mnist/updates",
    ):
        main(["index", "-m", "bare", os.fspath(path_obj)])
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


@pytest.mark.parametrize(
    "index_first,objects,annots",
    [
        (True, 32, 23),
        (False, 23, 23),
    ],
)
def test_add_current_workspace(
    index_first,
    objects,
    annots,
    workspace_path,
    ldb_instance,
):
    add_default_read_add_storage(ldb_instance)
    shutil.copytree(
        DATA_DIR / "fashion-mnist/original",
        "./train",
    )
    if index_first:
        main(["index", "-m", "bare", "."])
    ret = main(["add", "."])
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == objects
    assert num_empty_files(object_file_paths) == annots


@pytest.mark.parametrize(
    "index_first,objects,annots",
    [
        (True, 32, 23),
        (False, 23, 23),
    ],
)
def test_add_another_workspace(
    index_first,
    objects,
    annots,
    workspace_path,
    data_dir,
    ldb_instance,
    tmp_path,
):
    dir_to_add = os.fspath(data_dir / "fashion-mnist/original")
    other_workspace_path = tmp_path / "other-workspace"
    stage_new_workspace(other_workspace_path)
    os.chdir(other_workspace_path)
    if index_first:
        main(["index", "-m", "bare", dir_to_add])
    main(["add", dir_to_add])
    os.chdir(workspace_path)
    ret = main(["add", os.fspath(other_workspace_path)])

    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == objects
    assert num_empty_files(object_file_paths) == annots

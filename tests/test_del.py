import os
import shutil

import pytest

from ldb.core import add_default_read_add_storage
from ldb.main import main
from ldb.utils import DATASET_PREFIX, ROOT

from .data import QUERY_DATA
from .utils import (
    DATA_DIR,
    get_staged_object_file_paths,
    num_empty_files,
    stage_new_workspace,
)


@pytest.mark.parametrize("args,data_objs,annots", QUERY_DATA)
def test_del_root_dataset(
    args,
    data_objs,
    annots,
    fashion_mnist_session,
    global_workspace_path,
):
    main(["add", f"{DATASET_PREFIX}{ROOT}"])
    ret = main(["del", f"{DATASET_PREFIX}{ROOT}", *args])
    object_file_paths = get_staged_object_file_paths(global_workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 32 - data_objs
    assert num_empty_files(object_file_paths) == 23 - annots


def test_del_storage_location(workspace_path, staged_ds_fashion):
    ret = main(["del", os.fspath(DATA_DIR / "fashion-mnist/updates")])
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 23
    assert num_empty_files(object_file_paths) == 14


def test_del_data_objects(workspace_path, staged_ds_fashion):
    ret = main(
        [
            "del",
            "0x3c679fd1b8537dc7da1272a085e388e6",
            "0x982814b9116dce7882dfc31636c3ff7a",
            "0xebbc6c0cebb66738942ee56513f9ee2f",
            "0x1e0759182b328fd22fcdb5e6beb54adf",
        ],
    )
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 28
    assert num_empty_files(object_file_paths) == 21


def test_del_datasets(workspace_path, ds_a, ds_b, staged_ds_fashion):
    ret = main(["del", ds_a, ds_b])
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 25
    assert num_empty_files(object_file_paths) == 20


@pytest.mark.parametrize(
    "before,after,n_obj,n_annot",
    [
        ([], [], 15, 6),
        (["--limit", "12"], [], 26, 17),
        ([], ["--limit", "12"], 20, 11),
    ],
)
def test_del_root_dataset_query(
    before,
    after,
    n_obj,
    n_annot,
    workspace_path,
    staged_ds_fashion,
):
    ret = main(
        [
            "del",
            f"{DATASET_PREFIX}{ROOT}",
            *before,
            "--query",
            "label != `null` && label > `1` && label < `8`",
            *after,
        ],
    )
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == n_obj
    assert num_empty_files(object_file_paths) == n_annot


def test_del_workspace_dataset_query(workspace_path, staged_ds_fashion):
    ret = main(
        [
            "del",
            "--query",
            "label != `null` && label > `1` && label < `8`",
        ],
    )
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 15
    assert num_empty_files(object_file_paths) == 6


def test_del_current_workspace(
    workspace_path,
    ldb_instance,
    staged_ds_fashion,
):
    add_default_read_add_storage(ldb_instance)
    shutil.copytree(
        DATA_DIR / "fashion-mnist/original/has_both/train",
        "./train",
    )
    ret = main(["del", "."])
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 19
    assert num_empty_files(object_file_paths) == 10


def test_del_another_workspace(
    workspace_path,
    ldb_instance,
    tmp_path,
    staged_ds_fashion,
):
    other_workspace_path = tmp_path / "other-workspace"
    stage_new_workspace(other_workspace_path)
    os.chdir(other_workspace_path)
    main(
        ["add", os.fspath(DATA_DIR / "fashion-mnist/original/has_both/train")],
    )
    os.chdir(workspace_path)
    ret = main(["del", os.fspath(other_workspace_path)])

    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 19
    assert num_empty_files(object_file_paths) == 10

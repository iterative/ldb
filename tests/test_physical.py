import json
import os

import pytest

from ldb.main import main
from ldb.utils import DATASET_PREFIX, ROOT

from .data import LABEL_STUDIO_ANNOTATIONS, QUERY_DATA
from .utils import (
    DATA_DIR,
    get_staged_object_file_paths,
    get_workspace_counts,
    num_empty_files,
)


@pytest.mark.parametrize(
    "args,data_objs,annots",
    QUERY_DATA.values(),
    ids=QUERY_DATA.keys(),
)
def test_add_del_physical_root_dataset(
    args,
    data_objs,
    annots,
    fashion_mnist_session,
    global_workspace_path,
):
    main(["add", f"{DATASET_PREFIX}{ROOT}", "--physical"])
    ret = main(["del", f"{DATASET_PREFIX}{ROOT}", "--physical", *args])
    object_file_paths = get_staged_object_file_paths(global_workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 32 - data_objs
    assert num_empty_files(object_file_paths) == 23 - annots
    assert get_workspace_counts(global_workspace_path) == (32 - data_objs, 23 - annots)


def test_add_del_physical_data_objects(workspace_path, index_original):
    cmd_args = [
        "add",
        "id:3c679fd1b8537dc7da1272a085e388e6",
        "id:982814b9116dce7882dfc31636c3ff7a",
        "id:ebbc6c0cebb66738942ee56513f9ee2f",
        "id:1e0759182b328fd22fcdb5e6beb54adf",
        "--physical",
    ]
    ret = main(cmd_args)
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 4
    assert num_empty_files(object_file_paths) == 2
    assert get_workspace_counts(workspace_path) == (4, 2)
    cmd_args = [
        "del",
        "id:3c679fd1b8537dc7da1272a085e388e6",
        "id:982814b9116dce7882dfc31636c3ff7a",
        "--physical",
    ]
    ret = main(cmd_args)
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 2
    assert num_empty_files(object_file_paths) == 1
    assert get_workspace_counts(workspace_path) == (2, 1)


def test_add_del_physical_datasets(workspace_path, ds_a, ds_b):
    main(["stage", f"{DATASET_PREFIX}c"])
    cmd_args = ["add", ds_a, ds_b, "--physical"]
    ret = main(cmd_args)
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 7
    assert num_empty_files(object_file_paths) == 3
    assert get_workspace_counts(workspace_path) == (7, 3)
    cmd_args = ["del", ds_b, "--physical"]
    ret = main(cmd_args)
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 2
    assert num_empty_files(object_file_paths) == 1
    assert get_workspace_counts(workspace_path) == (2, 1)


def test_add_del_physical_root_dataset_query(workspace_path, index_original):
    cmd_args = [
        "add",
        f"{DATASET_PREFIX}{ROOT}",
        "--query",
        "label != `null` && label > `2` && label < `8`",
        "--physical",
    ]
    ret = main(cmd_args)
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 14
    assert num_empty_files(object_file_paths) == 14
    assert get_workspace_counts(workspace_path) == (14, 14)
    cmd_args = ["del", "--query", "label == `7`", "--physical"]
    ret = main(cmd_args)
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 11
    assert num_empty_files(object_file_paths) == 11
    assert get_workspace_counts(workspace_path) == (11, 11)


def test_instantiate_del_physical_bare(staged_ds_fashion, workspace_path):
    ret = main(["instantiate"])
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (32, 23)
    ret = main(
        [
            "del",
            "--query",
            "label != `null` && label > `1` && label < `8`",
            "--physical",
        ],
    )
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 15
    assert num_empty_files(object_file_paths) == 6
    assert get_workspace_counts(workspace_path) == (15, 6)


def test_instantiate_del_physical_strict(staged_ds_fashion, workspace_path):
    ret = main(["instantiate", "-m", "strict"])
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (23, 23)
    ret = main(
        [
            "del",
            "--query",
            "label != `null`",
            "--limit",
            "11",
            "--format",
            "strict",
            "--physical",
        ]
    )
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 21
    assert num_empty_files(object_file_paths) == 12
    assert get_workspace_counts(workspace_path) == (12, 12)


def test_add_del_physical_infer(workspace_path):
    main(["stage", "ds:my-dataset"])
    main(["index", "-m", "infer", os.fspath(DATA_DIR / "inferred/multilevel")])
    ret = main(["add", f"{DATASET_PREFIX}{ROOT}", "--physical", "--format", "infer"])
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (23, 0)
    ret = main(["del", "--limit", "11", "--format", "infer", "--physical"])
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 12
    assert num_empty_files(object_file_paths) == 12
    assert get_workspace_counts(workspace_path) == (12, 0)


def test_instantiate_del_physical_annot(staged_ds_fashion, workspace_path):
    ret = main(["instantiate", "-m", "annot"])
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (0, 23)
    ret = main(
        [
            "del",
            "--query",
            "label != `null`",
            "--limit",
            "11",
            "--format",
            "annot",
            "--physical",
        ]
    )
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 21
    assert num_empty_files(object_file_paths) == 12
    assert get_workspace_counts(workspace_path) == (0, 12)


def test_add_del_physical_annot_single_file(workspace_path, index_original):
    # Test add physical single file
    cmd_args = [
        "add",
        "id:3c679fd1b8537dc7da1272a085e388e6",
        "id:982814b9116dce7882dfc31636c3ff7a",
        "id:ebbc6c0cebb66738942ee56513f9ee2f",
        "id:1e0759182b328fd22fcdb5e6beb54adf",
        "--physical",
        "-m",
        "annot",
        "-p",
        "single-file=true",
    ]
    ret = main(cmd_args)
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 4
    assert num_empty_files(object_file_paths) == 2
    assert get_workspace_counts(workspace_path) == (0, 1)

    with open(os.path.join(workspace_path, "dataset.json"), encoding="utf-8") as f:
        annotations = sorted(json.load(f), key=lambda a: a["annotation"]["label"])  # type: ignore # noqa: E501

    assert len(annotations) == 2
    assert annotations[0]["annotation"]["label"] == 4
    assert annotations[0]["data-object-info"]["md5"] == "982814b9116dce7882dfc31636c3ff7a"
    assert annotations[1]["annotation"]["label"] == 7
    assert annotations[1]["data-object-info"]["md5"] == "ebbc6c0cebb66738942ee56513f9ee2f"

    # Test remove annotations from that single file
    cmd_args = [
        "del",
        "id:3c679fd1b8537dc7da1272a085e388e6",
        "id:982814b9116dce7882dfc31636c3ff7a",
        "--physical",
        "--format",
        "annot",
        "--param",
        "single-file=true",
        "--physical",
    ]
    ret = main(cmd_args)
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 2
    assert num_empty_files(object_file_paths) == 1
    assert get_workspace_counts(workspace_path) == (0, 1)

    with open(os.path.join(workspace_path, "dataset.json"), encoding="utf-8") as f:
        annotations = sorted(json.load(f), key=lambda a: a["annotation"]["label"])  # type: ignore # noqa: E501

    assert len(annotations) == 1
    assert annotations[0]["annotation"]["label"] == 7
    assert annotations[0]["data-object-info"]["md5"] == "ebbc6c0cebb66738942ee56513f9ee2f"

    # Test add more annotations to that single file (preserving existing annotations)
    cmd_args = [
        "add",
        "id:3c679fd1b8537dc7da1272a085e388e6",
        "id:982814b9116dce7882dfc31636c3ff7a",
        "--physical",
        "-m",
        "annot",
        "-p",
        "single-file=true",
    ]
    ret = main(cmd_args)
    object_file_paths = get_staged_object_file_paths(workspace_path)
    assert ret == 0
    assert len(object_file_paths) == 4
    assert num_empty_files(object_file_paths) == 2
    assert get_workspace_counts(workspace_path) == (0, 1)

    with open(os.path.join(workspace_path, "dataset.json"), encoding="utf-8") as f:
        annotations = sorted(json.load(f), key=lambda a: a["annotation"]["label"])  # type: ignore # noqa: E501

    assert len(annotations) == 2
    assert annotations[0]["annotation"]["label"] == 4
    assert annotations[0]["data-object-info"]["md5"] == "982814b9116dce7882dfc31636c3ff7a"
    assert annotations[1]["annotation"]["label"] == 7
    assert annotations[1]["data-object-info"]["md5"] == "ebbc6c0cebb66738942ee56513f9ee2f"


def test_add_del_physical_label_studio(workspace_path, index_original_for_label_studio):
    # Test add physical label studio, which is a single file of annotations
    cmd_args = [
        "add",
        "id:982814b9116dce7882dfc31636c3ff7a",
        "id:ebbc6c0cebb66738942ee56513f9ee2f",
        "--physical",
        "-m",
        "label-studio",
    ]
    ret = main(cmd_args)
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (0, 1)

    with open(os.path.join(workspace_path, "annotations.json"), encoding="utf-8") as f:
        annotations = sorted(json.load(f), key=lambda a: a["data"]["label"])  # type: ignore

    assert len(annotations) == 2
    assert annotations[0] == LABEL_STUDIO_ANNOTATIONS["25"]
    assert annotations[1] == LABEL_STUDIO_ANNOTATIONS["47"]

    # Test remove annotations from that single file
    cmd_args = [
        "del",
        "id:ebbc6c0cebb66738942ee56513f9ee2f",
        "--physical",
        "-m",
        "label-studio",
    ]
    ret = main(cmd_args)
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (0, 1)

    with open(os.path.join(workspace_path, "annotations.json"), encoding="utf-8") as f:
        annotations = sorted(json.load(f), key=lambda a: a["data"]["label"])  # type: ignore

    assert len(annotations) == 1
    assert annotations[0] == LABEL_STUDIO_ANNOTATIONS["25"]

    # Test add more annotations to that single file (preserving existing annotations)
    cmd_args = [
        "add",
        "id:b056f7ef766d698aee2542150f1add72",
        "--physical",
        "-m",
        "label-studio",
    ]
    ret = main(cmd_args)
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (0, 1)

    with open(os.path.join(workspace_path, "annotations.json"), encoding="utf-8") as f:
        annotations = sorted(json.load(f), key=lambda a: a["data"]["label"])  # type: ignore

    assert len(annotations) == 2
    assert annotations[0] == LABEL_STUDIO_ANNOTATIONS["70"]
    assert annotations[1] == LABEL_STUDIO_ANNOTATIONS["25"]

import json
import os
import os.path
import sys
from typing import List

import pytest

from ldb.exceptions import LDBException
from ldb.instantiate import modify_single_annot
from ldb.main import main
from ldb.typing import JSONDecoded
from ldb.utils import DATASET_PREFIX, ROOT

from .data import LABEL_STUDIO_ANNOTATIONS
from .utils import DATA_DIR, SCRIPTS_DIR, get_workspace_counts


def test_instantiate_bare(staged_ds_fashion, workspace_path):
    ret = main(["instantiate"])
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (32, 23)


def test_instantiate_bare_path(tmp_path, staged_ds_fashion, workspace_path):
    dest = os.fspath(tmp_path / "data")
    ret = main(["instantiate", "-t", dest])
    assert ret == 0
    assert get_workspace_counts(dest) == (32, 23)


def test_instantiate_with_transforms(
    staged_ds_fashion_with_transforms,
    workspace_path,
    transform_infos,
):
    ret = main(["instantiate"])
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (43, 17)


def test_instantiate_with_apply(staged_ds_fashion, workspace_path):
    ret = main(
        [
            "instantiate",
            "--apply",
            sys.executable,
            os.fspath(SCRIPTS_DIR / "random_predictions.py"),
        ],
    )
    annot_path = next(f for f in os.listdir() if f.endswith(".json"))
    with open(annot_path, encoding="utf-8") as f:
        annot = json.load(f)
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (32, 23)
    assert isinstance(annot.get("prediction"), float)


def test_instantiate_path_with_apply(
    tmp_path,
    staged_ds_fashion,
    workspace_path,
):
    dest = os.fspath(tmp_path / "data")
    ret = main(
        [
            "instantiate",
            "--apply",
            sys.executable,
            os.fspath(SCRIPTS_DIR / "random_predictions.py"),
            "-t",
            dest,
        ],
    )
    annot_path = os.path.join(
        dest,
        next(f for f in os.listdir(dest) if f.endswith(".json")),
    )
    with open(annot_path, encoding="utf-8") as f:
        annot = json.load(f)
    assert ret == 0
    assert get_workspace_counts(dest) == (32, 23)
    assert isinstance(annot.get("prediction"), float)


def test_instantiate_bare_path_without_parents(
    tmp_path,
    staged_ds_fashion,
    workspace_path,
):
    dest = os.fspath(tmp_path / "data/data")
    ret = main(["instantiate", "-t", dest])
    assert ret == 1
    assert not os.path.exists(dest)


def test_instantiate_strict(staged_ds_fashion, workspace_path):
    ret = main(["instantiate", "-m", "strict"])
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (23, 23)


def test_instantiate_with_query(staged_ds_fashion, workspace_path):
    ret = main(
        [
            "instantiate",
            "id:93e2a847c9341054107d8e93a259a9c8",
            "id:982814b9116dce7882dfc31636c3ff7a",
            "id:47149106168f7d88fcea9e168608f129",
            "id:a2430513e897d5abcf62a55b8df81355",
            "id:b5fba326c8247d9e62aa17a109146c02",
            "id:e299594dc1f79f8e69c6d79a42699822",
            "--query",
            "@ == `null` || type(inference.label) == 'number' && inference.label == label",
        ],
    )
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (3, 2)


def test_instantiate_annot(staged_ds_fashion, workspace_path):
    ret = main(["instantiate", "-m", "annot"])
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (0, 23)


def test_instantiate_infer(workspace_path):
    main(["stage", "ds:my-dataset"])
    main(["index", "-m", "infer", os.fspath(DATA_DIR / "inferred/multilevel")])
    main(["add", f"{DATASET_PREFIX}{ROOT}"])
    ret = main(["instantiate", "-m", "infer"])
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (23, 0)


def test_instantiate_annot_single_file(workspace_path, index_original):
    cmd_args = [
        "add",
        "id:3c679fd1b8537dc7da1272a085e388e6",
        "id:982814b9116dce7882dfc31636c3ff7a",
        "id:ebbc6c0cebb66738942ee56513f9ee2f",
        "id:1e0759182b328fd22fcdb5e6beb54adf",
    ]
    ret = main(cmd_args)
    assert ret == 0
    cmd_args = ["instantiate", "-m", "annot", "-p", "single-file=true"]
    ret = main(cmd_args)
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (0, 1)

    with open(os.path.join(workspace_path, "dataset.json"), encoding="utf-8") as f:
        annotations = sorted(json.load(f), key=lambda a: a["annotation"]["label"])  # type: ignore # noqa: E501

    assert len(annotations) == 2
    assert annotations[0]["annotation"]["label"] == 4
    assert annotations[0]["data-object-info"]["md5"] == "982814b9116dce7882dfc31636c3ff7a"
    assert annotations[1]["annotation"]["label"] == 7
    assert annotations[1]["data-object-info"]["md5"] == "ebbc6c0cebb66738942ee56513f9ee2f"


def test_modify_single_annot(tmp_path):
    filename = os.path.join(tmp_path, "dataset.json")

    original_list: List[JSONDecoded] = [
        {
            "data-object-info": {
                "md5": "ebbc6c0cebb66738942ee56513f9ee2f",
            }
        },
        {
            "data-object-info": {
                "md5": "1e0759182b328fd22fcdb5e6beb54adf",
            }
        },
    ]
    add_list: List[JSONDecoded] = [
        {
            "data-object-info": {
                "md5": "982814b9116dce7882dfc31636c3ff7a",
            }
        }
    ]
    remove_list: List[JSONDecoded] = [
        {
            "data-object-info": {
                "md5": "1e0759182b328fd22fcdb5e6beb54adf",
            }
        }
    ]
    expected_list: List[JSONDecoded] = [
        {
            "data-object-info": {
                "md5": "982814b9116dce7882dfc31636c3ff7a",
            }
        },
        {
            "data-object-info": {
                "md5": "ebbc6c0cebb66738942ee56513f9ee2f",
            }
        },
    ]

    # Test without dataset.json
    assert modify_single_annot(filename, filename, [], []) == (0, 0)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump({"property": "Not a top-level array"}, f)

    # Test invalid dataset.json
    with pytest.raises(LDBException):
        modify_single_annot(filename, filename, [], [])

    with open(filename, "w", encoding="utf-8") as f:
        json.dump([{"data": "missing"}], f)

    # Test invalid add
    with pytest.raises(ValueError):
        modify_single_annot(filename, filename, [{"no": "hash"}], [])

    # Test invalid remove
    with pytest.raises(ValueError):
        modify_single_annot(filename, filename, [], [{"no": "hash"}])

    # Test invalid original
    with pytest.raises(ValueError):
        modify_single_annot(filename, filename, [], [])

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(original_list, f)

    # Test full valid original
    assert modify_single_annot(filename, filename, add_list, remove_list) == (1, 1)

    with open(filename, encoding="utf-8") as f:
        new_annotations = json.load(f)

    assert sorted(new_annotations, key=lambda a: a["data-object-info"]["md5"]) == expected_list  # type: ignore # noqa: E501

    # Test delete dataset.json if all annotations removed
    assert modify_single_annot(filename, filename, [], expected_list) == (0, 2)

    assert os.path.exists(filename) is False


def test_instantiate_label_studio(workspace_path, index_original_for_label_studio):
    cmd_args = [
        "add",
        "id:982814b9116dce7882dfc31636c3ff7a",
        "id:ebbc6c0cebb66738942ee56513f9ee2f",
    ]
    ret = main(cmd_args)
    assert ret == 0
    cmd_args = ["instantiate", "-m", "label-studio"]
    ret = main(cmd_args)
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (0, 1)

    with open(os.path.join(workspace_path, "annotations.json"), encoding="utf-8") as f:
        annotations = sorted(json.load(f), key=lambda a: a["data"]["label"])  # type: ignore

    assert len(annotations) == 2
    assert annotations[0] == LABEL_STUDIO_ANNOTATIONS["25"]
    assert annotations[1] == LABEL_STUDIO_ANNOTATIONS["47"]

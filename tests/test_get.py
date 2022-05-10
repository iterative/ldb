import json
import os
import sys

from ldb.main import main
from ldb.utils import DATASET_PREFIX, ROOT, chdir

from .utils import DATA_DIR, SORT_DIR, get_workspace_counts


def test_get_ds_root_staged(staged_ds_fashion, workspace_path):
    with chdir(workspace_path):
        ret = main(["get", f"{DATASET_PREFIX}{ROOT}"])
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (32, 23)


def test_get_paths_with_new_instance(ldb_instance, tmp_path):
    paths = [
        os.fspath(DATA_DIR / "fashion-mnist/original"),
        os.fspath(DATA_DIR / "fashion-mnist/updates"),
    ]
    dest = os.fspath(tmp_path / "data")
    ret = main(["get", *paths, "-t", dest])
    assert ret == 0
    assert get_workspace_counts(dest) == (23, 23)


def test_get_with_apply(staged_ds_fashion, workspace_path):
    # pylint: disable=duplicate-code
    ret = main(
        [
            "get",
            f"{DATASET_PREFIX}{ROOT}",
            "--apply",
            sys.executable,
            os.fspath(SORT_DIR / "random_predictions.py"),
        ],
    )
    annot_path = next(f for f in os.listdir() if f.endswith(".json"))
    with open(annot_path, encoding="utf-8") as f:
        annot = json.load(f)
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (32, 23)
    assert isinstance(annot.get("prediction"), float)

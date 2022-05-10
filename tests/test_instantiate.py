import json
import os
import sys

from ldb.main import main
from ldb.utils import DATASET_PREFIX, ROOT

from .utils import DATA_DIR, SORT_DIR, get_workspace_counts


def test_instantiate_bare(staged_ds_fashion, workspace_path):
    ret = main(["instantiate"])
    assert ret == 0
    assert get_workspace_counts(workspace_path) == (32, 23)


def test_instantiate_bare_path(tmp_path, staged_ds_fashion, workspace_path):
    dest = os.fspath(tmp_path / "data")
    ret = main(["instantiate", "-t", dest])
    assert ret == 0
    assert get_workspace_counts(dest) == (32, 23)


def test_instantiate_with_apply(staged_ds_fashion, workspace_path):
    ret = main(
        [
            "instantiate",
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
            os.fspath(SORT_DIR / "random_predictions.py"),
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
    main(["instantiate", "-m", "strict"])
    assert get_workspace_counts(workspace_path) == (23, 23)


def test_instantiate_annot(staged_ds_fashion, workspace_path):
    main(["instantiate", "-m", "annot"])
    assert get_workspace_counts(workspace_path) == (0, 23)


def test_instantiate_infer(workspace_path):
    main(["stage", "ds:my-dataset"])
    main(["index", "-m", "infer", os.fspath(DATA_DIR / "inferred/multilevel")])
    main(["add", f"{DATASET_PREFIX}{ROOT}"])
    main(["instantiate", "-m", "infer"])
    assert get_workspace_counts(workspace_path) == (23, 0)

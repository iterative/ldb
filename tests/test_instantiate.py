import os
from pathlib import Path
from typing import List, Tuple, Union

from ldb.main import main
from ldb.utils import DATASET_PREFIX, ROOT
from ldb.workspace import iter_workspace_dir

from .utils import DATA_DIR


def get_workspace_counts(workspace_path: Union[str, Path]) -> Tuple[int, int]:
    all_files: List[str] = []
    for entry in iter_workspace_dir(workspace_path):
        if entry.is_file():
            all_files.append(entry.name)
        else:
            for _, _, files in os.walk(entry.path):
                all_files.extend(files)
    num_annotations = sum(f.endswith(".json") for f in all_files)
    num_data_objects = len(all_files) - num_annotations
    return num_data_objects, num_annotations


def test_instantiate_bare(staged_ds_fashion, workspace_path):
    main(["instantiate"])
    assert get_workspace_counts(workspace_path) == (32, 23)


def test_instantiate_bare_path(tmp_path, staged_ds_fashion, workspace_path):
    dest = os.fspath(tmp_path / "data")
    ret = main(["instantiate", dest])
    assert ret == 0
    assert get_workspace_counts(dest) == (32, 23)


def test_instantiate_bare_path_without_parents(
    tmp_path,
    staged_ds_fashion,
    workspace_path,
):
    dest = os.fspath(tmp_path / "data/data")
    ret = main(["instantiate", dest])
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

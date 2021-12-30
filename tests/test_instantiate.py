from pathlib import Path
from typing import Tuple

from ldb.main import main
from ldb.workspace import iter_workspace_dir


def get_workspace_counts(workspace_path: Path) -> Tuple[int, int]:
    all_files = list(iter_workspace_dir(workspace_path))
    num_annotations = sum(f.name.endswith(".json") for f in all_files)
    num_data_objects = len(all_files) - num_annotations
    return num_data_objects, num_annotations


def test_instantiate_bare(staged_ds_fashion, workspace_path):
    main(["instantiate"])
    assert get_workspace_counts(workspace_path) == (32, 23)


def test_instantiate_strict(staged_ds_fashion, workspace_path):
    main(["instantiate", "-m", "strict"])
    assert get_workspace_counts(workspace_path) == (23, 23)


def test_instantiate_annot(staged_ds_fashion, workspace_path):
    main(["instantiate", "-m", "annot"])
    assert get_workspace_counts(workspace_path) == (0, 23)

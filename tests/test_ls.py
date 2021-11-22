import os
import re
from pathlib import Path

from ldb.ls import ls
from ldb.main import main
from ldb.stage import stage_workspace
from ldb.utils import current_time
from ldb.workspace import WorkspaceDataset


def is_hash(s: str) -> bool:
    return re.fullmatch("[a-f0-9]{32}", s) is not None


def test_ls_workspace_dataset(tmp_path, data_dir, ldb_instance):
    workspace_path = tmp_path / "workspace"
    dirs_to_add = [
        data_dir / "fashion-mnist/original/has_both/train",
        data_dir / "fashion-mnist/original/data_objects_only/00011.png",
        data_dir / "fashion-mnist/updates",
    ]
    stage_workspace(
        workspace_path,
        WorkspaceDataset(
            dataset_name="my-dataset",
            staged_time=current_time(),
            parent="",
            tags=[],
        ),
    )
    os.chdir(workspace_path)
    for dir_path in dirs_to_add:
        main(["add", f"{os.fspath(dir_path)}"])

    ds_listings = ls(ldb_instance, workspace_path)
    annot_versions = [d.annotation_version for d in ds_listings]
    # fsspec's LocalFileSystem._strip_protocol does some normalization during
    # indexing, so we cast everything to Path objects for comparison
    paths = [Path(d.data_object_path) for d in ds_listings]

    expected_annot_versions = [1, 1, 1, 2, 1, 1, 1, 1, 2, 2, 1, 1, 0, 2]
    expected_str_paths = [
        "original/has_both/train/00016.png",
        "updates/no_inference/00028.png",
        "updates/no_inference/00023.png",
        "updates/same_inference/00029.png",
        "updates/no_inference/00010.png",
        "updates/no_inference/00021.png",
        "original/has_both/train/00038.png",
        "original/has_both/train/00007.png",
        "updates/diff_inference/00015.png",
        "updates/same_inference/00040.png",
        "original/has_both/train/00033.png",
        "updates/no_inference/00026.png",
        "original/data_objects_only/00011.png",
        "updates/diff_inference/00002.png",
    ]
    expected_paths = [
        data_dir / "fashion-mnist" / p for p in expected_str_paths
    ]
    assert annot_versions == expected_annot_versions
    assert paths == expected_paths
    assert all(is_hash(d.data_object_hash) for d in ds_listings)
    assert all(
        is_hash(d.annotation_hash)
        if d.annotation_version
        else d.annotation_hash == ""
        for d in ds_listings
    )

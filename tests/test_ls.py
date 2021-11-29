import os
import re
from pathlib import Path

from ldb.ls import ls_collection
from ldb.main import main
from ldb.path import WorkspacePath
from ldb.workspace import collection_dir_to_object

from .utils import stage_new_workspace


def is_hash(s: str) -> bool:
    return re.fullmatch("[a-f0-9]{32}", s) is not None


def test_ls_collection_with_workspace_dataset(
    tmp_path,
    data_dir,
    ldb_instance,
):
    workspace_path = tmp_path / "workspace"
    dirs_to_add = [
        data_dir / "fashion-mnist/original/has_both/train",
        data_dir / "fashion-mnist/original/data_objects_only/00011.png",
        data_dir / "fashion-mnist/updates",
    ]
    stage_new_workspace(workspace_path)
    os.chdir(workspace_path)
    for dir_path in dirs_to_add:
        main(["add", f"{os.fspath(dir_path)}"])

    ws_collection = collection_dir_to_object(
        workspace_path / WorkspacePath.COLLECTION,
    )
    ds_listings = ls_collection(ldb_instance, ws_collection)
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

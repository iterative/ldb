import os
from pathlib import Path

import fsspec

from ldb.exceptions import LDBException
from ldb.path import WorkspacePath
from ldb.utils import load_data_file


def status(workspace_path: Path):
    workspace_path = Path(os.path.normpath(workspace_path))
    workspace_dataset_path = workspace_path / WorkspacePath.DATASET
    try:
        workspace_ds = load_data_file(workspace_dataset_path)
    except FileNotFoundError as exc:
        raise LDBException(
            "No workspace dataset staged at "
            f"{repr(os.fspath(workspace_path))}",
        ) from exc
    ds_name = workspace_ds["dataset_name"]
    collection_dir_path = workspace_path / WorkspacePath.COLLECTION
    num_data_objects = 0
    num_annotations = 0
    for file in fsspec.open_files(os.fspath(collection_dir_path / "*/*")):
        with file as open_file:
            if open_file.read():
                num_annotations += 1
        num_data_objects += 1
    print(
        f"On ds:{ds_name} in {repr(os.fspath(workspace_path))}\n"
        f"  Num data objects: {num_data_objects:8d}\n"
        f"  Num annotations:  {num_annotations:8d}",
    )

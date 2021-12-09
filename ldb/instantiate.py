import os
import shutil
import tempfile
from pathlib import Path
from typing import Tuple

import fsspec

from ldb.path import InstanceDir, WorkspacePath
from ldb.utils import get_fsspec_path_suffix, get_hash_path, load_data_file
from ldb.workspace import collection_dir_to_object, ensure_empty_workspace


def instantiate(
    ldb_dir: Path,
    workspace_path: Path,
    force: bool = False,
) -> Tuple[int, int]:
    collection_obj = collection_dir_to_object(
        workspace_path / WorkspacePath.COLLECTION,
    )

    # fail fast if workspace is not empty
    ensure_empty_workspace(workspace_path, force)

    tmp_dir_base = workspace_path / WorkspacePath.TMP
    tmp_dir_base.mkdir(exist_ok=True)
    tmp_dir = Path(tempfile.mkdtemp(dir=tmp_dir_base))

    # annotations are small and stored in ldb; copy them first
    num_annotations = 0
    for data_object_hash, annotation_hash in collection_obj.items():
        if annotation_hash:
            dest = tmp_dir / (data_object_hash + ".json")
            user_annotation_file_path = (
                get_hash_path(
                    ldb_dir / InstanceDir.ANNOTATIONS,
                    annotation_hash,
                )
                / "user"
            )
            shutil.copy2(user_annotation_file_path, dest)
            num_annotations += 1

    for data_object_hash in collection_obj:
        data_object_dir = get_hash_path(
            ldb_dir / InstanceDir.DATA_OBJECT_INFO,
            data_object_hash,
        )
        data_object_meta = load_data_file(data_object_dir / "meta")

        path = data_object_meta["fs"]["path"]
        dest = tmp_dir / (data_object_hash + get_fsspec_path_suffix(path))

        fs = fsspec.filesystem(data_object_meta["fs"]["protocol"])
        fs.get_file(path, dest)

    # check again to make sure nothing was added while writing to the
    # temporary location
    ensure_empty_workspace(workspace_path, force)
    for path in tmp_dir.iterdir():
        shutil.move(os.fspath(path), os.fspath(workspace_path))

    tmp_dir.rmdir()
    return len(collection_obj), num_annotations

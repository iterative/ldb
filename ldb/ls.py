import os
from dataclasses import dataclass
from pathlib import Path
from typing import List

from ldb.path import InstanceDir, WorkspacePath
from ldb.utils import get_hash_path, load_data_file
from ldb.workspace import collection_dir_to_object


@dataclass
class DatasetListing:
    data_object_hash: str
    data_object_path: str
    annotation_hash: str
    annotation_version: int


def ls(
    ldb_dir: Path,
    workspace_path: Path,
) -> List[DatasetListing]:
    workspace_path = Path(os.path.normpath(workspace_path))
    collection = collection_dir_to_object(
        workspace_path / WorkspacePath.COLLECTION,
    )
    result = []
    for data_object_hash, annotation_hash in collection.items():
        data_object_dir = get_hash_path(
            ldb_dir / InstanceDir.DATA_OBJECT_INFO,
            data_object_hash,
        )
        annotation_version = 0
        if annotation_hash:
            annotation_meta = load_data_file(
                data_object_dir / "annotations" / annotation_hash,
            )
            annotation_version = annotation_meta["version"]
        data_object_meta = load_data_file(data_object_dir / "meta")

        result.append(
            DatasetListing(
                data_object_hash=data_object_hash,
                data_object_path=data_object_meta["fs"]["path"],
                annotation_hash=annotation_hash or "",
                annotation_version=annotation_version,
            ),
        )
    return result
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ldb.path import InstanceDir, WorkspacePath
from ldb.stage import stage_workspace
from ldb.utils import current_time, load_data_file
from ldb.workspace import WorkspaceDataset

DATA_DIR = Path(__file__).parent.parent / "data"
SORT_DIR = Path(__file__).parent.parent / "sort_option"
DATA_OBJECT_KEYS = (
    "alternate_paths",
    "first_indexed",
    "fs",
    "last_indexed",
    "last_indexed_by",
    "tags",
    "type",
)
DATA_OBJECT_FS_KEYS = (
    "atime",
    "ctime",
    "fs_id",
    "gid",
    "mode",
    "mtime",
    "path",
    "protocol",
    "size",
    "uid",
)
ANNOTATION_META_KEYS = (
    "first_indexed_time",
    "last_indexed_time",
    "mtime",
    "version",
)
ANNOTATION_LDB_KEYS = (
    "schema_version",
    "user_version",
)


def is_data_object_meta_obj(data: Dict[str, Any]) -> bool:
    return (
        tuple(data.keys()) == DATA_OBJECT_KEYS
        and tuple(data["fs"].keys()) == DATA_OBJECT_FS_KEYS
    )


def is_data_object_meta(file_path: Path) -> bool:
    return is_data_object_meta_obj(load_data_file(file_path))


def is_annotation_meta(file_path: Path) -> bool:
    return (
        tuple(load_data_file(file_path).keys()) == ANNOTATION_META_KEYS
        and (file_path.parent.parent / "current").is_file()
    )


def is_annotation(dir_path: Path) -> bool:
    return (
        tuple(
            load_data_file(dir_path / "ldb"),
        )
        == ANNOTATION_LDB_KEYS
        and bool(load_data_file(dir_path / "user"))
    )


def get_data_object_meta_file_paths(ldb_instance: Path) -> List[Path]:
    return list((ldb_instance / InstanceDir.DATA_OBJECT_INFO).glob("*/*/meta"))


def get_annotation_meta_file_paths(ldb_instance: Path) -> List[Path]:
    return list(
        (ldb_instance / InstanceDir.DATA_OBJECT_INFO).glob(
            "*/*/annotations/*",
        ),
    )


def get_annotation_dir_paths(ldb_instance: Path) -> List[Path]:
    return list((ldb_instance / InstanceDir.ANNOTATIONS).glob("*/*"))


def get_indexed_data_paths(
    ldb_dir: Path,
) -> Tuple[List[Path], List[Path], List[Path]]:
    return (
        get_data_object_meta_file_paths(ldb_dir),
        get_annotation_meta_file_paths(ldb_dir),
        get_annotation_dir_paths(ldb_dir),
    )


def stage_new_workspace(
    path: Path,
    name: str = "my-dataset",
    staged_time: Optional[datetime] = None,
    parent: str = "",
    tags: Optional[List[str]] = None,
) -> None:
    if staged_time is None:
        staged_time = current_time()
    if tags is None:
        tags = []
    stage_workspace(
        path,
        WorkspaceDataset(
            dataset_name=name,
            staged_time=staged_time,
            parent=parent,
            tags=tags,
        ),
    )


def get_staged_object_file_paths(workspace_path: Path) -> List[Path]:
    return list((workspace_path / WorkspacePath.COLLECTION).glob("*/*"))


def num_empty_files(paths: Iterable[Path]) -> int:
    num = 0
    for path in paths:
        num += bool(path.read_text())
    return num

from dataclasses import asdict, dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

from ldb.path import InstanceDir
from ldb.utils import (
    format_datetime,
    get_hash_path,
    load_data_file,
    parse_datetime,
)


@dataclass
class CommitInfo:
    created_by: str
    commit_time: datetime
    commit_message: str

    @classmethod
    def parse(cls, attr_dict: Dict[str, str]) -> "CommitInfo":
        attr_dict = attr_dict.copy()
        return cls(
            commit_time=parse_datetime(attr_dict.pop("commit_time")),
            **attr_dict,
        )

    def format(self) -> Dict[str, str]:
        attr_dict = asdict(self)
        return dict(
            commit_time=format_datetime(attr_dict.pop("commit_time")),
            **attr_dict,
        )


@dataclass
class DatasetVersion:
    version: int
    parent: str
    collection: str
    tags: List[str]
    commit_info: CommitInfo

    @classmethod
    def parse(cls, attr_dict: Dict[str, Any]) -> "DatasetVersion":
        attr_dict = attr_dict.copy()
        return cls(
            commit_info=CommitInfo.parse(attr_dict.pop("commit_info")),
            **attr_dict,
        )

    def format(self) -> Dict[str, Any]:
        attr_dict = {f.name: getattr(self, f.name) for f in fields(self)}
        return dict(
            commit_info=attr_dict.pop("commit_info").format(),
            tags=attr_dict.pop("tags").copy(),
            **attr_dict,
        )


@dataclass
class Dataset:
    name: str
    created_by: str
    created: datetime
    versions: List[str]

    @classmethod
    def parse(cls, attr_dict: Dict[str, Any]) -> "Dataset":
        attr_dict = attr_dict.copy()
        created = parse_datetime(attr_dict.pop("created"))
        return cls(created=created, **attr_dict)

    def format(self) -> Dict[str, Any]:
        attr_dict = asdict(self)
        created = format_datetime(attr_dict.pop("created"))
        return dict(created=created, **attr_dict)


def get_collection(
    ldb_dir: Path,
    dataset_version_hash: str,
) -> Dict[str, Optional[str]]:
    dataset_version_obj = DatasetVersion.parse(
        load_data_file(
            get_hash_path(
                ldb_dir / InstanceDir.DATASET_VERSIONS,
                dataset_version_hash,
            ),
        ),
    )
    return load_data_file(
        get_hash_path(
            ldb_dir / InstanceDir.COLLECTIONS,
            dataset_version_obj.collection,
        ),
    )


def get_collection_dir_items(
    collection_dir: Path,
    is_workspace: bool = True,
) -> Generator[Tuple[str, Optional[str]], None, None]:
    annotation_hash_func = (
        get_workspace_collection_annotation_hash
        if is_workspace
        else get_root_collection_annotation_hash
    )
    for path in collection_dir.glob("*/*"):
        yield path.parent.name + path.name, annotation_hash_func(path)


def get_root_collection_annotation_hash(
    data_object_path: Path,
) -> Optional[str]:
    try:
        return (data_object_path / "current").read_text()
    except FileNotFoundError:
        return None


def get_workspace_collection_annotation_hash(
    data_object_path: Path,
) -> Optional[str]:
    return data_object_path.read_text() or None

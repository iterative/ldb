from dataclasses import asdict, dataclass, fields
from datetime import datetime
from typing import Any, Dict, List, Optional

from ldb.utils import format_datetime, parse_datetime


@dataclass
class CommitInfo:
    created_by: str
    commit_time: datetime
    commit_message: str

    @classmethod
    def parse(cls, attr_dict: Dict[str, str]) -> "CommitInfo":
        attr_dict = attr_dict.copy()
        commit_time = parse_datetime(attr_dict.pop("commit_time"))
        return cls(commit_time=commit_time, **attr_dict)

    def format(self) -> Dict[str, str]:
        attr_dict = asdict(self)
        commit_time = format_datetime(attr_dict.pop("commit_time"))
        return dict(commit_time=commit_time, **attr_dict)


@dataclass
class DatasetVersion:
    version: int
    parent: Optional[str]
    collection: str
    tags: List[str]
    commit_info: CommitInfo

    @classmethod
    def parse(cls, attr_dict: Dict[str, Any]) -> "DatasetVersion":
        attr_dict = attr_dict.copy()
        commit_info = CommitInfo.parse(attr_dict.pop("commit_info"))
        return cls(commit_info=commit_info, **attr_dict)

    def format(self) -> Dict[str, Any]:
        attr_dict = {f.name: getattr(self, f.name) for f in fields(self)}
        commit_info = attr_dict.pop("commit_info").format()
        tags = attr_dict.pop("tags").copy()
        return dict(commit_info=commit_info, tags=tags, **attr_dict)


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

from dataclasses import dataclass
from typing import Any, Dict, List

from ldb.dataset import CommitInfo
from ldb.utils import hash_data, json_dumps


@dataclass
class DatasetVersion:
    version: int
    parent: str
    collection: str
    transform_mapping_id: str
    tags: List[str]
    commit_info: CommitInfo
    auto_pull: bool = False
    bytes: bytes = b""
    oid: str = ""

    def digest(self) -> None:
        self.bytes = json_dumps(self.format()).encode()
        self.oid = hash_data(self.bytes)

    @classmethod
    def parse(cls, attr_dict: Dict[str, Any]) -> "DatasetVersion":
        attr_dict = attr_dict.copy()
        return cls(
            commit_info=CommitInfo.parse(attr_dict.pop("commit_info")),
            **attr_dict,
        )

    def format(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "parent": self.parent,
            "collection": self.collection,
            "transform_mapping_id": self.transform_mapping_id,
            "tags": self.tags.copy(),
            "commit_info": self.commit_info.format(),
            "auto_pull": self.auto_pull,
        }

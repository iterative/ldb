from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List

from ldb.utils import format_datetime, json_dumps, parse_datetime


@dataclass
class Dataset:
    name: str
    created_by: str
    created: datetime
    versions: List[str]
    bytes: bytes = b""
    oid: str = ""

    def digest(self) -> None:
        self.bytes = json_dumps(self.format()).encode()
        self.oid = self.name

    @classmethod
    def parse(cls, attr_dict: Dict[str, Any]) -> "Dataset":
        attr_dict = attr_dict.copy()
        created = parse_datetime(attr_dict.pop("created"))
        versions = attr_dict.pop("versions").copy()
        return cls(created=created, versions=versions, **attr_dict)

    def format(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "created_by": self.created_by,
            "created": format_datetime(self.created),
            "versions": self.versions.copy(),
        }

    def numbered_versions(self) -> Dict[int, str]:
        return {i: v for i, v in enumerate(self.versions, 1) if v is not None}

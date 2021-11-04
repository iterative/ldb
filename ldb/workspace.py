from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List

from ldb.utils import format_datetime, parse_datetime


@dataclass
class WorkspaceDataset:
    dataset_name: str
    staged_time: datetime
    parent: str
    tags: List[str]

    @classmethod
    def parse(cls, attr_dict: Dict[str, Any]) -> "WorkspaceDataset":
        attr_dict = attr_dict.copy()
        return cls(
            staged_time=parse_datetime(attr_dict.pop("staged_time")),
            tags=attr_dict.pop("tags").copy(),
            **attr_dict,
        )

    def format(self) -> Dict[str, Any]:
        attr_dict = asdict(self)
        return dict(
            staged_time=format_datetime(attr_dict.pop("staged_time")),
            tags=attr_dict.pop("tags").copy(),
            **attr_dict,
        )

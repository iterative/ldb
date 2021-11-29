from datetime import datetime
from pathlib import Path
from typing import List, Optional

from ldb.stage import stage_workspace
from ldb.utils import current_time
from ldb.workspace import WorkspaceDataset

DATA_DIR = Path(__file__).parent.parent / "data"


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

from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

from ldb.path import WorkspacePath
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


def get_staged_object_file_paths(workspace_path: Path) -> List[Path]:
    return list((workspace_path / WorkspacePath.COLLECTION).glob("*/*"))


def num_empty_files(paths: Iterable[Path]) -> int:
    num = 0
    for path in paths:
        num += bool(path.read_text())
    return num

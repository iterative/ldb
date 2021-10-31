import json
import os
from datetime import datetime
from pathlib import Path

from ldb.exceptions import LDBException
from ldb.path import WorkspacePath
from ldb.utils import (
    format_datetime,
    parse_dataset_identifier,
    write_data_file,
)


def stage(
    _ldb_dir: Path,
    dataset: str,
    workspace_path: Path,
    _force: bool = False,
) -> None:
    workspace_path = Path(os.path.normpath(workspace_path))
    ds_name = parse_dataset_identifier(dataset)[0]
    if (
        workspace_path.is_dir()
        and next(workspace_path.iterdir(), None) is not None
    ):
        raise LDBException(
            f"Workspace is not empty {repr(os.fspath(workspace_path))}",
        )
    stage_new_workspace(ds_name, workspace_path)
    print(
        f"Staged new dataset ds:{ds_name} "
        f"at {repr(os.fspath(workspace_path))}",
    )


def stage_new_workspace(dataset_name: str, workspace_path: Path) -> None:
    current_timestamp = format_datetime(datetime.now())
    workspace_ds_obj = {
        "dataset_name": dataset_name,
        "staged_time": current_timestamp,
        "parent": None,
        "tags": [],
    }
    workspace_ds_obj_bytes = json.dumps(workspace_ds_obj).encode()

    workspace_path.mkdir(parents=True, exist_ok=True)
    (workspace_path / WorkspacePath.BASE).mkdir()
    (workspace_path / WorkspacePath.COLLECTION).mkdir()
    write_data_file(
        workspace_path / WorkspacePath.DATASET,
        workspace_ds_obj_bytes,
        overwrite_existing=True,
    )

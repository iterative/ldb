import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from ldb.exceptions import LDBException
from ldb.path import WorkspacePath
from ldb.utils import format_datetime, write_data_file


def stage(
    _ldb_dir: Path,
    dataset: str,
    workspace_path: Path,
    _force: bool = False,
):
    workspace_path = Path(os.path.normpath(workspace_path))
    ds_name = parse_dataset_identifier(dataset)[0]
    if ds_name is None:
        raise LDBException(
            'dataset must be in the form "ds:name" or "ds:name.vN"\n'
            'where "name" can contain characters in the group '
            "[A-Za-z0-9_-]\n"
            'and ".vN" denotes the version number (e.g. ".v1")',
        )
    workspace_ldb_base = workspace_path / WorkspacePath.BASE
    if (
        workspace_path.is_dir()
        and next(workspace_path.iterdir(), None) is not None
    ):
        raise LDBException(
            f"Workspace is not empty {repr(os.fspath(workspace_path))}",
        )

    current_timestamp = format_datetime(datetime.now())
    workspace_ds_obj = {
        "dataset_name": ds_name,
        "staged_time": current_timestamp,
        "parent": None,
        "tags": [],
    }
    workspace_ds_obj_bytes = json.dumps(workspace_ds_obj).encode()

    workspace_path.mkdir(parents=True, exist_ok=True)
    workspace_ldb_base.mkdir()
    (workspace_path / WorkspacePath.COLLECTION).mkdir()
    write_data_file(
        workspace_path / WorkspacePath.DATASET,
        workspace_ds_obj_bytes,
        overwrite_existing=True,
    )
    print(
        f"Staged new dataset {dataset} "
        f"at {repr(os.fspath(workspace_path))}",
    )


def parse_dataset_identifier(
    dataset: str,
) -> Tuple[Optional[str], Optional[int]]:
    match = re.search(r"^ds:([A-Za-z0-9_-]+)(?:\.v(\d+))?$", dataset)
    if match is None:
        return None, None
    name, version = match.groups()
    return name, int(version) if version is not None else None


def generate_dataset_identifier(
    name: str,
    version: Optional[int] = None,
):
    version_suffix = f".v{version}" if version is not None else ""
    return f"ds:{name}{version_suffix}"

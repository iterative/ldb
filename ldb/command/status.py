import argparse
import os
from pathlib import Path
from typing import Iterable

import shtab

from ldb.config import get_ldb_dir
from ldb.core import is_ldb_instance
from ldb.exceptions import LDBInstanceNotFoundError
from ldb.status import status


def status_command(options):
    ldb_dir = get_ldb_dir()
    if not is_ldb_instance(ldb_dir):
        raise LDBInstanceNotFoundError(
            f"No LDB instance at {os.fspath(ldb_dir)!r}",
        )
    ws_status = status(ldb_dir, options.path)
    if ws_status.dataset_name == "root":
        prefix = ""
    else:
        prefix = f"Workspace directory: {os.fspath(options.path)!r}\n\n"
    print(
        f"{prefix}"
        f"ds:{ws_status.dataset_name}\n"
        f"  Num data objects: {ws_status.num_data_objects:8d}\n"
        f"  Num annotations:  {ws_status.num_annotations:8d}",
    )


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "status",
        parents=parents,
        help="Get status of workspace",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "path",
        nargs="?",
        type=Path,
        default=".",
        help="Workspace path",
    ).complete = shtab.DIRECTORY
    parser.set_defaults(func=status_command)

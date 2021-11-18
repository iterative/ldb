import argparse
import os
from pathlib import Path
from typing import Iterable

import shtab

from ldb.core import get_ldb_instance
from ldb.diff import diff, format_summary, summarize_diff
from ldb.status import status
from ldb.workspace import load_workspace_dataset


def status_command(options):
    ldb_dir = get_ldb_instance()
    ws_status = status(ldb_dir, options.path)
    prefix = ""
    if ws_status.dataset_name != "root":
        prefix = f"Workspace directory: {os.fspath(options.path)!r}\n\n"
    print(
        f"{prefix}"
        f"ds:{ws_status.dataset_name}\n"
        f"  Num data objects: {ws_status.num_data_objects:8d}\n"
        f"  Num annotations:  {ws_status.num_annotations:8d}",
    )
    if ws_status.dataset_name != "root":
        workspace_ds = load_workspace_dataset(options.path)
        summary_items = summarize_diff(
            diff(ldb_dir, options.path, workspace_ds.parent),
        )
        print()
        if any(summary_items):
            print("Unsaved changes:")
            print(format_summary(*summary_items))
        else:
            print("No unsaved changes.")


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

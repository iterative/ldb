import argparse
import os
from argparse import Namespace
from pathlib import Path
from typing import Iterable

from ldb.core import get_ldb_instance
from ldb.dataset import get_collection_size
from ldb.diff import format_summary, simple_diff, summarize_diff
from ldb.path import WorkspacePath
from ldb.status import status
from ldb.utils import format_dataset_identifier
from ldb.workspace import load_workspace_dataset


def status_command(options: Namespace) -> None:
    dataset = options.dataset
    ldb_dir = get_ldb_instance()
    ws_status = status(ldb_dir, dataset)
    ds_identifier = format_dataset_identifier(
        ws_status.dataset_name,
        ws_status.dataset_version,
    )
    workspace_path = Path(".")
    prefix = ""
    if not dataset:
        prefix = f"Workspace directory: {os.fspath(workspace_path)!r}\n\n"
    print(
        f"{prefix}"
        f"{ds_identifier}\n"
        f"  Num data objects: {ws_status.num_data_objects:8d}\n"
        f"  Num annotations:  {ws_status.num_annotations:8d}",
    )
    if not dataset:
        workspace_ds = load_workspace_dataset(workspace_path)
        if workspace_ds.parent:
            summary_items = summarize_diff(
                simple_diff(ldb_dir, workspace_path, workspace_ds.parent),
            )
        else:
            collection_size = get_collection_size(
                workspace_path / WorkspacePath.COLLECTION,
            )
            summary_items = collection_size, 0, 0
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
        help="Get status of workspace or dataset",
    )
    parser.add_argument(
        "dataset",
        nargs="?",
        default="",
        help="Dataset to show status for",
    )
    parser.set_defaults(func=status_command)

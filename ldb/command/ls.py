import argparse
import os
from pathlib import Path
from typing import Iterable

import shtab

from ldb.config import get_ldb_dir
from ldb.core import is_ldb_instance
from ldb.exceptions import LDBInstanceNotFoundError
from ldb.ls import ls
from ldb.utils import format_dataset_identifier


def ls_command(options):
    ldb_dir = get_ldb_dir()
    if not is_ldb_instance(ldb_dir):
        raise LDBInstanceNotFoundError(f"{os.fspath(ldb_dir)!r}")
    ds_workspace, ds_listings = ls(ldb_dir, options.path)
    ds_ident = format_dataset_identifier(ds_workspace["dataset_name"])
    print(f"Listing {ds_ident} members:\n")
    print(f"{'Data Object Hash':37} {'Annot.':8} Data Object Path")
    for item in ds_listings:
        annotation_version = str(item.annotation_version or "-")
        path = (
            item.data_object_path
            if options.verbose
            else left_truncate(item.data_object_path)
        )
        print(f"  0x{item.data_object_hash:35} {annotation_version:8} {path}")


def left_truncate(item: str, max_len=36) -> str:
    return (
        item
        if len(item) <= max_len
        else "..." + item[-(max_len - 3) :]  # noqa: E203
    )


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "list",
        parents=parents,
        help="List objects and annotations",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "path",
        nargs="?",
        type=Path,
        default=".",
        help="Path or dataset to list",
    ).complete = shtab.DIRECTORY
    parser.set_defaults(func=ls_command)

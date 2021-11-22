import argparse
from argparse import Namespace
from typing import Iterable

import shtab

from ldb.config import get_ldb_dir
from ldb.core import init
from ldb.path import Filename
from ldb.storage import add_storage, create_storage_location


def add_storage_command(options: Namespace) -> None:
    ldb_dir = get_ldb_dir()
    if not ldb_dir.is_dir():
        ldb_dir = init(ldb_dir)
    storage_location = create_storage_location(
        path=options.path,
        read_and_add=options.read_add,
    )
    add_storage(
        ldb_dir / Filename.STORAGE,
        storage_location,
        force=options.force,
    )


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "add-storage",
        parents=parents,
        help="Add a storage location",
    )
    parser.add_argument(
        "-a",
        "--read-add",
        action="store_true",
        default=False,
        help="Use this location for adding objects",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Replace a subdirectory with one of its parents",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "path",
        help="The path to the location",
    ).complete = shtab.DIRECTORY
    parser.set_defaults(func=add_storage_command)

import argparse
from typing import Iterable

from ldb.config import get_ldb_dir
from ldb.core import init
from ldb.exceptions import LDBInstanceNotFoundError
from ldb.path import Filename
from ldb.storage import add_storage, create_storage_location


def add_storage_command(options):
    try:
        ldb_dir = get_ldb_dir()
    except LDBInstanceNotFoundError:
        print("No existing LDB instance found. Creating a new one.")
        ldb_dir = init()
    storage_location = create_storage_location(
        path=options.path,
        add=options.add,
    )
    add_storage(
        ldb_dir / Filename.STORAGE,
        storage_location,
        force=options.force,
    )
    print(f"Added storage location {repr(storage_location.path)}")


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
        "--add",
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
    parser.add_argument(
        "path",
        help="The path to the location",
    )
    parser.set_defaults(func=add_storage_command)

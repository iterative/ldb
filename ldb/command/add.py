import argparse
import os
from pathlib import Path
from typing import Iterable

import shtab

from ldb.add import add
from ldb.config import get_ldb_dir
from ldb.core import is_ldb_instance
from ldb.exceptions import LDBInstanceNotFoundError
from ldb.index import index


def add_command(options):
    ldb_dir = get_ldb_dir()
    if not is_ldb_instance(ldb_dir):
        raise LDBInstanceNotFoundError(f"{repr(os.fspath(ldb_dir))}")
    data_object_hashes = []
    for path in options.path:
        data_object_hashes.extend(index(path, ldb_dir))
    add(
        ldb_dir,
        Path("."),
        data_object_hashes,
    )


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "add",
        parents=parents,
        help="Add a data objects under a certain path",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "path",
        nargs="+",
        help="Storage location(s) to add",
    ).complete = shtab.FILE
    parser.set_defaults(func=add_command)

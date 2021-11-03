import argparse
import os
from typing import Iterable

import shtab

from ldb.config import get_ldb_dir
from ldb.core import is_ldb_instance
from ldb.exceptions import LDBInstanceNotFoundError
from ldb.index import index


def index_command(options):
    ldb_dir = get_ldb_dir()
    if not is_ldb_instance(ldb_dir):
        raise LDBInstanceNotFoundError(f"{repr(os.fspath(ldb_dir))}")
    index(ldb_dir, options.paths)


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "index",
        parents=parents,
        help="Index a storage location",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "paths",
        metavar="path",
        nargs="+",
        help="File, directory, or path prefix. Supports glob syntax.",
    ).complete = shtab.FILE
    parser.set_defaults(func=index_command)

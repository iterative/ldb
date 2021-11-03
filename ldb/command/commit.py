import argparse
import os
from pathlib import Path
from typing import Iterable

from ldb.commit import commit
from ldb.config import get_ldb_dir
from ldb.core import is_ldb_instance
from ldb.exceptions import LDBInstanceNotFoundError


def commit_command(options):
    ldb_dir = get_ldb_dir()
    if not is_ldb_instance(ldb_dir):
        raise LDBInstanceNotFoundError(f"{os.fspath(ldb_dir)!r}")
    commit(ldb_dir, Path("."), options.message)


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "commit",
        parents=parents,
        help="Commit the currently staged workspace dataset",
    )
    parser.add_argument(
        "message",
        nargs="?",
        default="",
        help="A message about this commit",
    )
    parser.set_defaults(func=commit_command)

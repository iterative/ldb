import argparse
from argparse import Namespace
from pathlib import Path
from typing import Iterable

from ldb.commit import commit
from ldb.core import get_ldb_instance


def commit_command(options: Namespace) -> None:
    commit(get_ldb_instance(), Path("."), options.message)


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

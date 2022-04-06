from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from ldb.commit import commit
from ldb.core import get_ldb_instance

if TYPE_CHECKING:
    from argparse import _SubParsersAction


def commit_command(options: Namespace) -> None:
    commit(get_ldb_instance(), Path("."), options.message)


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParser]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "commit",
        parents=parents,
        help="Commit the currently staged workspace dataset",
    )
    parser.add_argument(
        "message",
        metavar="<message>",
        nargs="?",
        default="",
        help="A message about this commit",
    )
    parser.set_defaults(func=commit_command)

from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from ldb.commit import commit
from ldb.core import get_ldb_instance

if TYPE_CHECKING:
    from argparse import _SubParsersAction


def commit_command(options: Namespace) -> None:
    commit(get_ldb_instance(), Path("."), options.dataset, options.message)


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
        "dataset",
        metavar="<dataset>",
        nargs="?",
        default="",
        help="Rename to this dataset name when committing",
    )
    parser.add_argument(
        "-m",
        "--message",
        metavar="<message>",
        default="",
        help="A message about this commit",
    )
    parser.set_defaults(func=commit_command)

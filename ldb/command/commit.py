from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from ldb.cli_utils import json_bool
from ldb.commit import commit
from ldb.core import get_ldb_instance

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def commit_command(options: Namespace) -> None:
    commit(
        get_ldb_instance(),
        Path("."),
        dataset_identifier=options.dataset,
        message=options.message,
        auto_pull=options.auto_pull,
    )


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
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
    parser.add_argument(
        "--auto-pull",
        default=None,
        choices=(True, False),
        type=json_bool,
        const=True,
        nargs="?",
        help="Set the auto-pull field on this dataset",
    )
    parser.set_defaults(func=commit_command)

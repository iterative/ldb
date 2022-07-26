from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from ldb.add import add
from ldb.cli_utils import add_data_obj_params

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def add_command(options: Namespace) -> None:
    add(
        Path("."),
        options.paths,
        options.query_args,
    )


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "add",
        parents=parents,
        help="Add a data objects under a certain path",
    )
    add_data_obj_params(parser, dest="query_args")
    parser.set_defaults(func=add_command)

from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Iterable

from ldb.add import delete_from_index
from ldb.cli_utils import add_data_obj_params

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def unindex_obj_command(options: Namespace) -> None:
    delete_from_index(
        options.paths,
        options.query_args,
    )


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "unindex",
        parents=parents,
        help="Permanently remove data object info from the ldb index",
    )

    add_data_obj_params(parser, dest="query_args")
    parser.set_defaults(func=unindex_obj_command)

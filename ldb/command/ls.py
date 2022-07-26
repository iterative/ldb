from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Iterable

from ldb.cli_utils import add_data_obj_params
from ldb.core import get_ldb_instance
from ldb.ls import ls, print_dataset_listings

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def ls_command(options: Namespace) -> None:
    ldb_dir = get_ldb_instance()
    ds_listings = ls(
        ldb_dir,
        options.paths,
        options.query_args,
        warn=False,
    )
    if options.summary:
        print(len(ds_listings))
    else:
        num_items = print_dataset_listings(
            ds_listings,
            verbose=options.verbose,
        )
        print(f"\n{num_items} matching data objects")


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "list",
        parents=parents,
        help="List objects and annotations",
    )
    parser.add_argument(
        "-s",
        "--summary",
        action="store_true",
        default=False,
        help="Show the number of objects instead of listing them out",
    )
    add_data_obj_params(parser, dest="query_args")
    parser.set_defaults(func=ls_command)

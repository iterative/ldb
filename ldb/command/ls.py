import argparse
from argparse import Namespace
from typing import Iterable

from ldb.cli_utils import add_data_obj_params
from ldb.core import get_ldb_instance
from ldb.ls import ls, print_dataset_listings


def ls_command(options: Namespace) -> None:
    ldb_dir = get_ldb_instance()
    ds_listings = ls(
        ldb_dir,
        options.paths,
        options.query_args,
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
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
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

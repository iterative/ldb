import argparse
from argparse import Namespace
from typing import Iterable

import shtab

from ldb.core import get_ldb_instance
from ldb.ls import ls, print_dataset_listings


def ls_command(options: Namespace) -> None:
    ldb_dir = get_ldb_instance()
    ds_listings = ls(ldb_dir, options.paths, options.query)
    print_dataset_listings(ds_listings, verbose=options.verbose)


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
        "--query",
        action="store",
        help="Overwrite an unsaved dataset",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "paths",
        metavar="path",
        nargs="*",
        help="Storage location, data object identifier, or dataset",
    ).complete = shtab.FILE
    parser.set_defaults(func=ls_command)

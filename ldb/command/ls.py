import argparse
from argparse import Namespace
from typing import Iterable

from ldb.cli_utils import add_data_object_arguments
from ldb.core import get_ldb_instance
from ldb.ls import ls, print_dataset_listings


def ls_command(options: Namespace) -> None:
    ldb_dir = get_ldb_instance()
    ds_listings = ls(
        ldb_dir,
        options.paths,
        options.annotation_query,
        options.file_query,
    )
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
    add_data_object_arguments(parser)
    parser.set_defaults(func=ls_command)

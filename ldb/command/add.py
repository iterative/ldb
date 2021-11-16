import argparse
from pathlib import Path
from typing import Iterable

import shtab

from ldb.add import add, get_arg_type, process_args_for_add
from ldb.core import get_ldb_instance


def add_command(options):
    ldb_dir = get_ldb_instance()
    data_object_hashes, annotation_hashes, message = process_args_for_add(
        ldb_dir,
        get_arg_type(options.paths),
        options.paths,
    )
    if message:
        print(message)
        print()
    print("Adding to working dataset...")
    add(
        ldb_dir,
        Path("."),
        data_object_hashes,
        annotation_hashes,
    )


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "add",
        parents=parents,
        help="Add a data objects under a certain path",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "paths",
        metavar="path",
        nargs="+",
        help="Storage location, data object identifier, or dataset",
    ).complete = shtab.FILE
    parser.set_defaults(func=add_command)

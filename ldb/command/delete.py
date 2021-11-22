import argparse
from argparse import Namespace
from pathlib import Path
from typing import Iterable

import shtab

from ldb.add import delete, get_arg_type, process_args_for_delete
from ldb.core import get_ldb_instance


def delete_command(options: Namespace) -> None:
    ldb_dir = get_ldb_instance()
    data_object_hashes = process_args_for_delete(
        ldb_dir,
        get_arg_type(options.paths),
        options.paths,
    )
    delete(
        Path("."),
        data_object_hashes,
    )


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "del",
        parents=parents,
        help="Delete data objects from workspace dataset",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "paths",
        metavar="path",
        nargs="+",
        help="Storage location, data object identifier, or dataset",
    ).complete = shtab.FILE
    parser.set_defaults(func=delete_command)

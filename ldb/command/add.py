import argparse
from argparse import Namespace
from pathlib import Path
from typing import Iterable

import shtab

from ldb.add import add, apply_query, get_arg_type, process_args_for_add
from ldb.core import get_ldb_instance
from ldb.query import get_bool_search_func


def add_command(options: Namespace) -> None:
    search = (
        get_bool_search_func(options.query)
        if options.query is not None
        else None
    )
    ldb_dir = get_ldb_instance()
    data_object_hashes, annotation_hashes, message = process_args_for_add(
        ldb_dir,
        get_arg_type(options.paths),
        options.paths,
    )
    if message:
        print(message)
        print()

    if search is None:
        collection = dict(zip(data_object_hashes, annotation_hashes))
    else:
        collection = apply_query(
            ldb_dir,
            search,
            data_object_hashes,
            annotation_hashes,
        )
    print("Adding to working dataset...")
    add(
        Path("."),
        collection,
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
    parser.add_argument(
        "--query",
        action="store",
        help="Overwrite an unsaved dataset",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "paths",
        metavar="path",
        nargs="+",
        help="Storage location, data object identifier, or dataset",
    ).complete = shtab.FILE
    parser.set_defaults(func=add_command)

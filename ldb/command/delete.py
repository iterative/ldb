import argparse
from argparse import Namespace
from pathlib import Path
from typing import Iterable

import shtab

from ldb.add import (
    ArgType,
    apply_query_to_data_objects,
    delete,
    get_arg_type,
    process_args_for_delete,
    process_args_for_ls,
)
from ldb.core import get_ldb_instance
from ldb.exceptions import LDBException
from ldb.query import get_bool_search_func


def delete_command(options: Namespace) -> None:
    search = (
        get_bool_search_func(options.query)
        if options.query is not None
        else None
    )

    paths = options.paths
    if search is None and not paths:
        raise LDBException("Must provide either a query or at least one path")
    ldb_dir = get_ldb_instance()
    if search is None:
        data_object_hashes: Iterable[str] = process_args_for_delete(
            ldb_dir,
            get_arg_type(paths),
            paths,
        )
    else:
        if not paths:
            paths = ["."]
            arg_type = ArgType.WORKSPACE_DATASET
        else:
            arg_type = get_arg_type(paths)
        data_object_hashes, annotation_hashes, _ = process_args_for_ls(
            ldb_dir,
            arg_type,
            paths,
        )
        data_object_hashes = apply_query_to_data_objects(
            ldb_dir,
            search,
            data_object_hashes,
            annotation_hashes,
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
    parser.add_argument(
        "--query",
        action="store",
        help="JMESPath query that filters annotations",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "paths",
        metavar="path",
        nargs="*",
        help="Storage location, data object identifier, or dataset",
    ).complete = shtab.FILE
    parser.set_defaults(func=delete_command)

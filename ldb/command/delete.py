import argparse
from argparse import Namespace
from pathlib import Path
from typing import Iterable

from ldb.add import delete, process_args_for_delete, process_args_for_ls
from ldb.cli_utils import add_data_object_arguments
from ldb.core import get_ldb_instance
from ldb.dataset import (
    apply_file_query_to_data_objects,
    apply_query_to_data_objects,
)
from ldb.exceptions import LDBException
from ldb.func_utils import apply_optional
from ldb.query.search import get_bool_search_func


def delete_command(options: Namespace) -> None:
    if (
        options.annotation_query is None
        and options.file_query is None
        and not options.paths
    ):
        raise LDBException("Must provide either a query or at least one path")
    ldb_dir = get_ldb_instance()
    search = apply_optional(get_bool_search_func, options.annotation_query)
    file_search = apply_optional(get_bool_search_func, options.file_query)
    paths = options.paths
    if search is None:
        data_object_hashes: Iterable[str] = process_args_for_delete(
            ldb_dir,
            paths,
        )
    else:
        data_object_hashes, annotation_hashes, _ = process_args_for_ls(
            ldb_dir,
            paths,
        )
        data_object_hashes = apply_query_to_data_objects(
            ldb_dir,
            search,
            data_object_hashes,
            annotation_hashes,
        )
    if file_search is not None:
        data_object_hashes = apply_file_query_to_data_objects(
            ldb_dir,
            file_search,
            data_object_hashes,
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
    add_data_object_arguments(parser)
    parser.set_defaults(func=delete_command)

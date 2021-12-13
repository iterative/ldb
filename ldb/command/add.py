import argparse
from argparse import Namespace
from pathlib import Path
from typing import Iterable

from ldb.add import add, process_args_for_add
from ldb.cli_utils import add_data_object_arguments
from ldb.core import get_ldb_instance
from ldb.dataset import apply_queries
from ldb.exceptions import LDBException
from ldb.func_utils import apply_optional
from ldb.query.search import get_bool_search_func
from ldb.utils import DATASET_PREFIX, ROOT


def add_command(options: Namespace) -> None:
    ldb_dir = get_ldb_instance()
    search = apply_optional(get_bool_search_func, options.annotation_query)
    file_search = apply_optional(get_bool_search_func, options.file_query)
    paths = options.paths
    if not paths:
        if search is None and file_search is None:
            raise LDBException(
                "Must provide either a query or at least one path",
            )
        paths = [f"{DATASET_PREFIX}{ROOT}"]
    data_object_hashes, annotation_hashes, message = process_args_for_add(
        ldb_dir,
        paths,
    )
    if message:
        print(message)
        print()

    collection = apply_queries(
        ldb_dir,
        search,
        file_search,
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
    add_data_object_arguments(parser)
    parser.set_defaults(func=add_command)

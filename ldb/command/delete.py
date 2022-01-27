import argparse
from argparse import Namespace
from pathlib import Path
from typing import Iterable

from ldb.add import delete, process_args_for_delete, process_args_for_ls
from ldb.cli_utils import add_data_obj_params
from ldb.core import get_ldb_instance
from ldb.dataset import apply_queries
from ldb.exceptions import LDBException


def delete_command(options: Namespace) -> None:
    paths = options.paths
    query_args = options.query_args
    if not paths:
        if not query_args:
            raise LDBException(
                "Must provide either a query or at least one path",
            )
    ldb_dir = get_ldb_instance()
    ldb_dir = get_ldb_instance()
    if not query_args:
        data_object_hashes: Iterable[str] = process_args_for_delete(
            ldb_dir,
            paths,
        )
    else:
        data_object_hashes, annotation_hashes, _ = process_args_for_ls(
            ldb_dir,
            paths,
        )
        collection = apply_queries(
            ldb_dir,
            data_object_hashes,
            annotation_hashes,
            query_args,
        )
        data_object_hashes = (d for d, _ in collection)
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
    add_data_obj_params(parser, dest="query_args")
    parser.set_defaults(func=delete_command)

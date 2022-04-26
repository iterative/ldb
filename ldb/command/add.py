from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from ldb.add import add, process_args_for_add
from ldb.cli_utils import add_data_obj_params
from ldb.core import get_ldb_instance
from ldb.dataset import apply_queries
from ldb.exceptions import LDBException
from ldb.utils import DATASET_PREFIX, ROOT

if TYPE_CHECKING:
    from argparse import _SubParsersAction


def add_command(options: Namespace) -> None:
    paths = options.paths
    query_args = options.query_args
    if not paths:
        if not query_args:
            raise LDBException(
                "Must provide either a query or at least one path",
            )
        paths = [f"{DATASET_PREFIX}{ROOT}"]
    ldb_dir = get_ldb_instance()
    data_object_hashes, annotation_hashes, message = process_args_for_add(
        ldb_dir,
        paths,
    )
    if message:
        print(message)
        print()

    collection = apply_queries(
        ldb_dir,
        data_object_hashes,
        annotation_hashes,
        query_args,
        warn=False,
    )
    print("Adding to working dataset...")
    add(
        Path("."),
        collection,
    )


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParser]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "add",
        parents=parents,
        help="Add a data objects under a certain path",
    )
    add_data_obj_params(parser, dest="query_args")
    parser.set_defaults(func=add_command)

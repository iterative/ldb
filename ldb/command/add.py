import argparse
from pathlib import Path
from typing import Iterable

import shtab

from ldb.add import add
from ldb.core import get_ldb_instance
from ldb.index import index


def add_command(options):
    ldb_dir = get_ldb_instance()
    print("Indexing paths...")
    indexing_result = index(ldb_dir, options.paths)
    print(indexing_result.summary())
    print()
    print("Adding to working dataset...")
    add(
        ldb_dir,
        Path("."),
        indexing_result.data_object_hashes,
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

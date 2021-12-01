import argparse
import json
from argparse import Namespace
from typing import Iterable

import shtab

from ldb.core import get_ldb_instance
from ldb.evaluate import evaluate


def evaluate_command(options: Namespace) -> None:
    for data_object_hash, result in evaluate(
        get_ldb_instance(),
        options.query,
        options.paths,
        use_file_attributes=options.file,
    ):
        print(f"0x{data_object_hash}")
        print(json.dumps(result, indent=2))


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "eval",
        parents=parents,
        help="Evaluate a query on the specified annotations",
    )
    parser.add_argument(
        "--file",
        action="store_true",
        default=False,
        help=(
            "Applies JMESPath query to file attributes instead of annotations"
        ),
    )
    parser.add_argument(
        "query",
        help="JMESPath query to apply",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "paths",
        metavar="path",
        nargs="*",
        help="Storage location, data object identifier, or dataset",
    ).complete = shtab.FILE
    parser.set_defaults(func=evaluate_command)

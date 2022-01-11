import argparse
import json
from argparse import Namespace
from typing import Iterable

from ldb.cli_utils import add_data_object_arguments
from ldb.core import get_ldb_instance
from ldb.evaluate import evaluate, process_query_args


def evaluate_command(options: Namespace) -> None:
    annotation_query, file_query = process_query_args(options.query_args)
    for data_object_hash, *results in evaluate(
        get_ldb_instance(),
        options.paths,
        annotation_query,
        file_query,
    ):
        if not options.json_only:
            print(f"0x{data_object_hash}")
        for item in results:
            print(json.dumps(item, indent=2))
        if not options.json_only:
            print()


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
        "-j",
        "--json-only",
        action="store_true",
        default=False,
        help="Show JSON output only instead of showing object hashes",
    )
    add_data_object_arguments(parser)
    parser.set_defaults(func=evaluate_command)

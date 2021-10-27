import argparse
from typing import Iterable

from ldb.index import index


def index_command(options):
    index(options.path)


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "index",
        parents=parents,
        help="Index a storage location",
    )
    parser.add_argument(
        "path",
        help="Directory or path prefix to index",
    )
    parser.set_defaults(func=index_command)

import argparse
from pathlib import Path
from typing import Iterable

from ldb.core import init


def init_command(options):
    init(options.path, force=options.force)


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "init",
        parents=parents,
        help="Initialize an empty ldb instance",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Overwrite an existing .ldb/ directory.",
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Directory in which to create .ldb/",
    )
    parser.set_defaults(func=init_command)

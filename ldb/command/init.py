import argparse
from pathlib import Path
from typing import Iterable

from ldb.config import set_default_instance
from ldb.core import init


def init_command(options):
    init(options.path, force=options.force)
    set_default_instance(options.path, overwrite_existing=False)


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "init",
        parents=parents,
        help="Initialize an empty LDB instance",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Overwrite an existing instance",
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Directory in which to initialize new instance",
    )
    parser.set_defaults(func=init_command)

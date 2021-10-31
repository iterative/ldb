import argparse
from pathlib import Path
from typing import Iterable

import shtab

from ldb.status import status


def status_command(options):
    status(options.path)


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "status",
        parents=parents,
        help="Get status of workspace",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "path",
        nargs="?",
        type=Path,
        default=".",
        help="Workspace path",
    ).complete = shtab.DIRECTORY
    parser.set_defaults(func=status_command)

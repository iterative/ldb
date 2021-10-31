import argparse
import os
from pathlib import Path
from typing import Iterable

import shtab

from ldb.status import status


def status_command(options):
    ws_status = status(options.path)
    print(
        f"On ds:{ws_status.dataset_name} in "
        f"{repr(os.fspath(options.path))}\n"
        f"  Num data objects: {ws_status.num_data_objects:8d}\n"
        f"  Num annotations:  {ws_status.num_annotations:8d}",
    )


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

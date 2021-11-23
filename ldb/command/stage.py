import argparse
from argparse import Namespace
from pathlib import Path
from typing import Iterable

from ldb.config import get_ldb_dir
from ldb.core import init_quickstart
from ldb.stage import stage


def stage_command(options: Namespace) -> None:
    ldb_dir = get_ldb_dir()
    if not ldb_dir.is_dir():
        ldb_dir = init_quickstart()
    stage(
        ldb_dir,
        options.dataset,
        options.workspace_path,
        options.force,
    )


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "stage",
        parents=parents,
        help="Stage a dataset in the current workspace",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Overwrite an unsaved dataset",
    )
    parser.add_argument(
        "dataset",
        help="Name of the dataset to stage",
    )
    parser.add_argument(
        "workspace_path",
        nargs="?",
        type=Path,
        default=".",
        help="Workspace directory to stage the dataset in",
    )
    parser.set_defaults(func=stage_command)

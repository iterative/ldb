from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Iterable

from ldb.cli_utils import add_target_dir_argument
from ldb.config import get_ldb_dir
from ldb.core import init_quickstart
from ldb.stage import stage

if TYPE_CHECKING:
    from argparse import _SubParsersAction


def stage_command(options: Namespace) -> None:
    ldb_dir = get_ldb_dir()
    if not ldb_dir.is_dir():
        ldb_dir = init_quickstart()
    stage(
        ldb_dir,
        options.dataset,
        options.target_dir,
        options.force,
    )


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParser]",
    parents: Iterable[ArgumentParser],
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
    add_target_dir_argument(parser)
    parser.add_argument(
        "dataset",
        metavar="<dataset>",
        help="Name of the dataset to stage",
    )
    parser.set_defaults(func=stage_command)

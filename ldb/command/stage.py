from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Iterable

from ldb.cli_utils import add_target_dir_argument
from ldb.stage import stage

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def stage_command(options: Namespace) -> None:
    stage(
        options.dataset,
        options.target_dir,
        force=options.force,
    )


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
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

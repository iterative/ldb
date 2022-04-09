from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from ldb.cli_utils import add_data_obj_params
from ldb.core import get_ldb_instance
from ldb.pull import pull

if TYPE_CHECKING:
    from argparse import _SubParsersAction


def pull_command(options: Namespace) -> None:
    ldb_dir = get_ldb_instance()
    pull(
        ldb_dir,
        Path("."),
        options.paths,
        options.query_args,
    )


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParser]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "pull",
        parents=parents,
        help="Update annotation versions",
    )
    add_data_obj_params(parser, dest="query_args")
    parser.set_defaults(func=pull_command)

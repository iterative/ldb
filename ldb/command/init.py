from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

import shtab

from ldb.config import set_default_instance
from ldb.core import init

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def init_command(options: Namespace) -> None:
    init(
        options.path,
        force=options.force,
        read_any_cloud_location=False,
        db_type=options.database,
        physical_workflow=options.default_physical,
    )
    set_default_instance(options.path, overwrite_existing=False)


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
    parents: Iterable[ArgumentParser],
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
        "-d",
        "--database",
        default="file",
        choices=("file",),
        help="Database backend to use",
    )
    parser.add_argument(
        "--default-physical",
        action="store_true",
        default=False,
        help=(
            "Use physical workflow by default to auto-instantiate/delete data objects "
            "and annotations"
        ),
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "path",
        metavar="<path>",
        type=Path,
        help="Directory in which to initialize new instance",
    ).complete = shtab.DIRECTORY
    parser.set_defaults(func=init_command)

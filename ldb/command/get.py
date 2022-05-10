from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from ldb.cli_utils import (
    add_data_format_arguments,
    add_data_obj_params,
    add_target_dir_argument,
)
from ldb.core import get_ldb_instance
from ldb.data_formats import INSTANTIATE_FORMATS, Format
from ldb.get import get

if TYPE_CHECKING:
    from argparse import _SubParsersAction


def instantiate_command(options: Namespace) -> None:
    ldb_dir = get_ldb_instance()
    workspace_path = Path(options.target_dir)
    get(
        workspace_path,
        workspace_path,
        options.paths,
        options.query_args,
        fmt=options.format,
        force=options.force,
        apply=options.apply,
        ldb_dir=ldb_dir,
    )


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParser]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "get",
        parents=parents,
        help="Get the specified data objects",
    )
    add_data_obj_params(parser, dest="query_args")
    add_data_format_arguments(
        parser,
        default=Format.BARE,
        formats=INSTANTIATE_FORMATS,
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Remove existing workspace contents",
    )
    add_target_dir_argument(parser)
    parser.add_argument(
        "--apply",
        nargs="+",
        metavar="<exec>",
        default=None,
        dest="apply",
        help="Executable to apply to data objects and annotations",
    )
    parser.set_defaults(func=instantiate_command)

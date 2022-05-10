from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from ldb.cli_utils import add_data_obj_params, add_instantiate_arguments
from ldb.get import get

if TYPE_CHECKING:
    from argparse import _SubParsersAction


def instantiate_command(options: Namespace) -> None:
    workspace_path = Path(options.target_dir)
    get(
        workspace_path,
        workspace_path,
        options.paths,
        options.query_args,
        fmt=options.format,
        force=options.force,
        apply=options.apply,
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
    add_instantiate_arguments(parser)
    parser.set_defaults(func=instantiate_command)

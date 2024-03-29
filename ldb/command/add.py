from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from ldb.add import add
from ldb.cli_utils import (
    add_data_obj_params,
    add_physical_workflow_arguments,
    physical_workflow_format,
    using_physical_workflow,
)

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def add_command(options: Namespace) -> None:
    result = add(
        Path("."),
        options.paths,
        options.query_args,
        physical_workflow=using_physical_workflow(options.physical),
        fmt=physical_workflow_format(options.format),
        params=dict(options.params),
    )
    print(result.summary())


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "add",
        parents=parents,
        help="Add data object(s) from a certain path",
    )
    add_physical_workflow_arguments(parser)
    add_data_obj_params(parser, dest="query_args")
    parser.set_defaults(func=add_command)

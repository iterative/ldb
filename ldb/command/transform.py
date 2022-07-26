from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from ldb.cli_utils import ExtendAction, add_data_obj_params, simple_name_list
from ldb.exceptions import LDBException
from ldb.transform import UpdateType, add_transform

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def transform_command(options: Namespace) -> None:
    ran_update = False
    if options.set:
        if options.add or options.remove:
            raise ValueError("Cannot use --add or --del along with --set")
        add_transform(
            Path("."),
            options.paths,
            options.query_args,
            options.set,
            update_type=UpdateType.SET,
        )
        ran_update = True
    else:
        if options.add:
            add_transform(
                Path("."),
                options.paths,
                options.query_args,
                options.add,
                update_type=UpdateType.ADD,
            )
            ran_update = True
        if options.remove:
            add_transform(
                Path("."),
                options.paths,
                options.query_args,
                options.remove,
                update_type=UpdateType.DEL,
            )
            ran_update = True
    if not ran_update:
        raise LDBException(
            "Must provide at least one transform update option: --add, "
            "--del, or --set",
        )


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "transform",
        parents=parents,
        help=(
            "Change the sets of transforms assigned to data objects in the "
            "current working dataset"
        ),
    )
    add_data_obj_params(parser, dest="query_args")
    parser.add_argument(
        "-a",
        "--add",
        metavar="<transforms>",
        default=[],
        action=ExtendAction,
        type=simple_name_list,
        help="Comma-separated set of transform names to add",
    )
    parser.add_argument(
        "-r",
        "--remove",
        metavar="<transforms>",
        default=[],
        action=ExtendAction,
        type=simple_name_list,
        help="Comma-separated set of transform names to remove if present",
    )
    parser.add_argument(
        "-s",
        "--set",
        metavar="<transforms>",
        default=[],
        action=ExtendAction,
        type=simple_name_list,
        help=(
            "Comma-separated set of transform names to assign. A matching "
            "data object's entire transform set will be replaced with this "
            "one"
        ),
    )
    parser.set_defaults(func=transform_command)

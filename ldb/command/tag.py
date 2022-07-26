from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Iterable

from ldb.add import select_data_object_hashes
from ldb.cli_utils import ExtendAction, add_data_obj_params, simple_name_list
from ldb.core import get_ldb_instance
from ldb.data_object_tag import tag_data_objects
from ldb.exceptions import LDBException
from ldb.utils import DATASET_PREFIX, ROOT

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def tag_command(options: Namespace) -> None:
    paths = options.paths
    query_args = options.query_args
    if not paths:
        if not query_args:
            raise LDBException(
                "Must provide either a query or at least one path",
            )
        paths = [f"{DATASET_PREFIX}{ROOT}"]
    if options.set:
        if options.add or options.remove:
            raise LDBException("Cannot use --add or --remove along with --set")
        set_tags = options.set
    else:
        set_tags = None
    ldb_dir = get_ldb_instance()
    data_object_hashes = select_data_object_hashes(
        ldb_dir,
        paths,
        query_args,
        warn=False,
    )
    tag_data_objects(
        ldb_dir,
        data_object_hashes,
        add_tags=options.add,
        remove_tags=options.remove,
        set_tags=set_tags,
    )


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "tag",
        parents=parents,
        help="Tag data objects",
    )
    add_data_obj_params(parser, dest="query_args")
    parser.add_argument(
        "-a",
        "--add",
        metavar="<tags>",
        default=[],
        type=simple_name_list,
        action=ExtendAction,
        help="Comma-separated list of tags to add to data objects",
    )
    parser.add_argument(
        "-r",
        "--remove",
        metavar="<tags>",
        default=[],
        type=simple_name_list,
        action=ExtendAction,
        help="Comma-separated list of tags to remove from data objects",
    )
    parser.add_argument(
        "-s",
        "--set",
        metavar="<tags>",
        default=[],
        type=simple_name_list,
        action=ExtendAction,
        help=(
            "Comma-separated list of tags to set for data objects. A matching "
            "data object's entire tag set will be replaced with this one"
        ),
    )
    parser.set_defaults(func=tag_command)

from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Iterable

from ldb.add import select_data_object_hashes
from ldb.cli_utils import add_data_obj_params
from ldb.core import get_ldb_instance
from ldb.data_object_tag import tag_data_objects
from ldb.exceptions import LDBException
from ldb.utils import DATASET_PREFIX, ROOT

if TYPE_CHECKING:
    from argparse import _SubParsersAction


def tag_command(options: Namespace) -> None:
    paths = options.paths
    query_args = options.query_args
    if not paths:
        if not query_args:
            raise LDBException(
                "Must provide either a query or at least one path",
            )
        paths = [f"{DATASET_PREFIX}{ROOT}"]
    ldb_dir = get_ldb_instance()
    data_object_hashes = select_data_object_hashes(
        ldb_dir,
        paths,
        query_args,
    )
    tag_data_objects(
        ldb_dir,
        data_object_hashes,
        add_tags=[t for lst in options.add for t in lst],
        remove_tags=[t for lst in options.remove for t in lst],
    )


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParser]",
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
        default=[],
        metavar="<tag>",
        nargs="*",
        action="append",
    )
    parser.add_argument(
        "-r",
        "--remove",
        default=[],
        metavar="<tag>",
        nargs="*",
        action="append",
    )
    parser.set_defaults(func=tag_command)

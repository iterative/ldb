import json
from argparse import ArgumentParser, Namespace
from json.decoder import JSONDecodeError
from typing import TYPE_CHECKING, Iterable, Tuple

import shtab

from ldb.cli_utils import json_bool
from ldb.config import get_ldb_dir
from ldb.core import init
from ldb.path import Filename
from ldb.storage import FSOptions, add_storage, create_storage_location

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def add_storage_command(options: Namespace) -> None:
    ldb_dir = get_ldb_dir()
    if not ldb_dir.is_dir():
        ldb_dir = init(ldb_dir)
    storage_location = create_storage_location(
        path=options.path,
        read_and_add=options.read_add,
        options=parse_fs_options(options.fs_options),
    )
    add_storage(
        ldb_dir / Filename.STORAGE,
        storage_location,
        force=options.force,
    )


def parse_fs_options(
    fs_options: Iterable[Tuple[str, str]],
) -> FSOptions:
    result = {}
    for k, v in fs_options:
        try:
            result[k] = json.loads(v)
        except JSONDecodeError:
            result[k] = v
    return result


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "add-storage",
        parents=parents,
        help="Add a storage location",
    )
    parser.add_argument(
        "-a",
        "--read-add",
        action="store",
        default=False,
        type=json_bool,
        help="Use this location for adding objects",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Replace a subdirectory with one of its parents",
    )
    parser.add_argument(
        "-o",
        "--option",
        metavar=("<key>", "<value>"),
        default=[],
        action="append",
        dest="fs_options",
        nargs=2,
        help=(
            "Additional filesystem option where <key> is a string and <value> "
            "is a json value. May be used multiple times"
        ),
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "path",
        metavar="<path>",
        help="The path to the location",
    ).complete = shtab.DIRECTORY
    parser.set_defaults(func=add_storage_command)

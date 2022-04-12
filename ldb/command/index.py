from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Iterable

import shtab

from ldb import config
from ldb.cli_utils import add_data_format_arguments, tag_list
from ldb.config import ConfigType
from ldb.core import get_ldb_instance
from ldb.data_formats import INDEX_FORMATS, Format
from ldb.index import index

if TYPE_CHECKING:
    from argparse import _SubParsersAction


def index_command(options: Namespace) -> None:
    tags = [t for lst in options.tags for t in lst]
    ldb_dir = get_ldb_instance()
    result = index(
        ldb_dir,
        options.paths,
        read_any_cloud_location=(
            (
                config.load_first([ConfigType.INSTANCE])  # type: ignore[call-overload] # noqa: E501
                or {}
            )
            .get("core", {})
            .get("read_any_cloud_location", False)
        ),
        fmt=options.format,
        tags=tags,
    )
    print(result.summary())


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParser]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "index",
        parents=parents,
        help="Index a storage location",
    )
    add_data_format_arguments(
        parser,
        default=Format.AUTO,
        formats=INDEX_FORMATS,
    )
    parser.add_argument(
        "--add-tags",
        metavar="<tags>",
        dest="tags",
        default=[],
        type=tag_list,
        action="append",
        help="Comma-separated list of tags to add to indexed data objects",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "paths",
        metavar="<path>",
        nargs="+",
        help="File, directory, or path prefix. Supports glob syntax.",
    ).complete = shtab.FILE
    parser.set_defaults(func=index_command)

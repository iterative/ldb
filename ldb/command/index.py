import argparse
from argparse import Namespace
from typing import Iterable

import shtab

from ldb import config
from ldb.cli_utils import add_data_format_arguments
from ldb.config import ConfigType
from ldb.core import get_ldb_instance
from ldb.data_formats import INDEX_FORMATS, Format
from ldb.index import index


def index_command(options: Namespace) -> None:
    ldb_dir = get_ldb_instance()
    result = index(
        ldb_dir,
        options.paths,
        read_any_cloud_location=(
            (
                config.load_first([ConfigType.INSTANCE])  # type: ignore[union-attr,call-overload] # noqa: E501
                or {}
            )
            .get("core", {})
            .get("read_any_cloud_location", False)
        ),
        fmt=options.format,
    )
    print(result.summary())


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
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
    parser.add_argument(  # type: ignore[attr-defined]
        "paths",
        metavar="path",
        nargs="+",
        help="File, directory, or path prefix. Supports glob syntax.",
    ).complete = shtab.FILE
    parser.set_defaults(func=index_command)

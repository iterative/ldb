import argparse
from argparse import Namespace
from typing import Iterable

import shtab

from ldb import config
from ldb.config import ConfigType
from ldb.core import get_ldb_instance
from ldb.index import FORMATS, index


def index_command(options: Namespace) -> None:
    ldb_dir = get_ldb_instance()
    print("Indexing paths...")
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


def choice_str(choices: Iterable[str]) -> str:
    choice_strings = ",".join(str(c) for c in choices)
    return f"{{{choice_strings}}}"


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "index",
        parents=parents,
        help="Index a storage location",
    )
    parser.add_argument(
        "-f",
        "--format",
        default="auto",
        metavar="<format>",
        choices=FORMATS,
        help=(
            "Format of the given storage location. "
            f"Options: {choice_str(FORMATS)}"
        ),
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "paths",
        metavar="path",
        nargs="+",
        help="File, directory, or path prefix. Supports glob syntax.",
    ).complete = shtab.FILE
    parser.set_defaults(func=index_command)

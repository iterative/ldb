import re
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, List, Tuple

from ldb.cli_utils import add_data_obj_params
from ldb.core import get_ldb_instance
from ldb.pull import pull

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT

VERSION_RE = r"^v(\d+)$"


def pull_command(options: Namespace) -> None:
    ldb_dir = get_ldb_instance()
    paths, version = get_version_and_paths(options.paths)
    pull(
        ldb_dir,
        Path("."),
        paths,
        options.query_args,
        version,
        warn=False,
    )


def get_version_and_paths(paths: List[str]) -> Tuple[List[str], int]:
    if paths:
        version = get_version(paths[-1])
        if version != -1:
            paths = paths[:-1]
    else:
        version = -1
    return paths, version


def get_version(s: str) -> int:
    match = re.search(VERSION_RE, s)
    if match is not None:
        return int(match.group(1))
    return -1


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
    parents: Iterable[ArgumentParser],
) -> None:
    help_msg = "Update annotation versions."
    desc = (
        "By default each annotation is updated to the newest version. If the "
        "final positional argument is a version identifier (e.g. v2), it will "
        "be used instead."
    )
    parser = subparsers.add_parser(
        "pull",
        parents=parents,
        help=help_msg,
        description=f"{help_msg} {desc}",
    )
    add_data_obj_params(parser, dest="query_args")
    parser.set_defaults(func=pull_command)

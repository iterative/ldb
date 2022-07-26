from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Iterable

import shtab

from ldb import config
from ldb.cli_utils import (
    add_data_format_arguments,
    add_param_option,
    choice_str,
    simple_name_list,
)
from ldb.config import ConfigType
from ldb.core import get_ldb_instance
from ldb.data_formats import INDEX_FORMATS, Format
from ldb.index import index
from ldb.index.utils import AnnotMergeStrategy

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


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
        annot_merge_strategy=options.annot_merge_strategy,
        params=dict(options.params),
        ephemeral_remote=options.ephemeral_remote,
    )
    print(result.summary())


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
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
        type=simple_name_list,
        action="append",
        help="Comma-separated list of tags to add to indexed data objects",
    )
    annot_update_choice_str = choice_str([s.value for s in AnnotMergeStrategy])
    parser.add_argument(
        "--annotation-update",
        metavar="<strategy>",
        dest="annot_merge_strategy",
        default=AnnotMergeStrategy.REPLACE,
        choices=[s.value for s in AnnotMergeStrategy],
        help=(
            "Merge strategy for combining a data object's current annotation "
            "with the one discovered during indexing. "
            f"Choices: {annot_update_choice_str}"
        ),
    )
    add_param_option(parser)
    parser.add_argument(
        "--ephemeral-remote",
        action="store_true",
        default=False,
        help=(
            "Allow non-storage cloud files to be indexed. They will be "
            "copied to read-add storage."
        ),
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "paths",
        metavar="<path>",
        nargs="+",
        help="File, directory, or path prefix. Supports glob syntax.",
    ).complete = shtab.FILE
    parser.set_defaults(func=index_command)

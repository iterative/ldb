from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Iterable

from ldb.core import get_ldb_instance
from ldb.diff import (
    DiffItem,
    DiffType,
    format_summary,
    full_diff,
    simple_diff,
    summarize_diff,
)
from ldb.string_utils import left_truncate
from ldb.utils import DATA_OBJ_ID_PREFIX

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def diff_command(options: Namespace) -> None:
    ldb_dir = get_ldb_instance()
    items = list(
        simple_diff(
            ldb_dir,
            options.dataset1,
            options.dataset2,
            ".",
        ),
    )
    if not options.summary:
        for diff_item in full_diff(ldb_dir, items):
            row = format_diff_item(diff_item, options.verbose)
            if row:
                print(row)
    summary_items = summarize_diff(items)
    if any(summary_items):
        if not options.summary:
            print()
        print(format_summary(*summary_items))


def format_diff_item(diff_item: DiffItem, verbose: bool) -> str:
    if diff_item.diff_type == DiffType.ADDITION:
        prefix = "+"
        annotation_col = annotation_version_str(diff_item.annotation_version2)
    elif diff_item.diff_type == DiffType.DELETION:
        prefix = "-"
        annotation_col = annotation_version_str(diff_item.annotation_version1)
    elif diff_item.diff_type == DiffType.MODIFICATION:
        prefix = "m"
        annotation_col = (
            f"{annotation_version_str(diff_item.annotation_version1)} "
            f"-> {annotation_version_str(diff_item.annotation_version2)}"
        )
    else:
        return ""
    data_object_path = (
        diff_item.data_object_path
        if verbose
        else left_truncate(diff_item.data_object_path)
    )
    return (
        f"{prefix} {DATA_OBJ_ID_PREFIX}{diff_item.data_object_hash:35} "
        f"{annotation_col:12} {data_object_path}"
    )


def annotation_version_str(annotation_version: int) -> str:
    return str(annotation_version) if annotation_version else "-"


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "diff",
        parents=parents,
        help="Show diff of two datasets",
    )
    parser.add_argument(
        "dataset1",
        metavar="<dataset>",
        nargs="?",
        default="",
        help="Dataset to start with",
    )
    parser.add_argument(
        "dataset2",
        metavar="<dataset>",
        nargs="?",
        default="",
        help="Dataset to show changes for",
    )
    parser.add_argument(
        "-s",
        "--summary",
        action="store_true",
        default=False,
        help="Show only the number of additions, deletions and modifications.",
    )
    parser.set_defaults(func=diff_command)

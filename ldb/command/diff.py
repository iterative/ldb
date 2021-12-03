import argparse
from argparse import Namespace
from pathlib import Path
from typing import Iterable

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


def diff_command(options: Namespace) -> None:
    ldb_dir = get_ldb_instance()
    items = list(
        simple_diff(
            ldb_dir,
            Path("."),
            options.dataset1,
            options.dataset2,
        ),
    )
    for diff_item in full_diff(ldb_dir, items):
        row = format_diff_item(diff_item, options.verbose)
        if row:
            print(row)
    summary_items = summarize_diff(items)
    if any(summary_items):
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
        f"{prefix} 0x{diff_item.data_object_hash:35} "
        f"{annotation_col:12} {data_object_path}"
    )


def annotation_version_str(annotation_version: int) -> str:
    return str(annotation_version) if annotation_version else "-"


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
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
    parser.set_defaults(func=diff_command)

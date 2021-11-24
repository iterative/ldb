import argparse
from argparse import Namespace
from itertools import repeat
from typing import Iterable

import shtab

from ldb.add import ArgType, get_arg_type, process_args_for_ls
from ldb.core import get_ldb_instance
from ldb.dataset import get_annotations
from ldb.ls import ls
from ldb.query import get_bool_search_func
from ldb.string_utils import left_truncate


def ls_command(options: Namespace) -> None:
    search = (
        get_bool_search_func(options.query)
        if options.query is not None
        else None
    )
    ldb_dir = get_ldb_instance()
    if not options.paths:
        paths = ["."]
        arg_type = ArgType.WORKSPACE_DATASET
    else:
        paths = options.paths
        arg_type = get_arg_type(options.paths)

    data_object_hashes, annotation_hashes, _ = process_args_for_ls(
        ldb_dir,
        arg_type,
        paths,
    )
    if annotation_hashes is None:
        annotation_hashes = repeat("")
    if search is None:
        collection = dict(zip(data_object_hashes, annotation_hashes))
    else:
        collection = {
            data_object_hash: annotation_hash
            for data_object_hash, annotation_hash, keep in zip(
                data_object_hashes,
                annotation_hashes,
                search(get_annotations(ldb_dir, annotation_hashes)),
            )
            if keep
        }

    ds_listings = ls(ldb_dir, collection)
    print(f"{'Data Object Hash':37} {'Annot.':8} Data Object Path")
    for item in ds_listings:
        annotation_version = str(item.annotation_version or "-")
        path = (
            item.data_object_path
            if options.verbose
            else left_truncate(item.data_object_path)
        )
        print(f"  0x{item.data_object_hash:35} {annotation_version:8} {path}")


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "list",
        parents=parents,
        help="List objects and annotations",
    )
    parser.add_argument(
        "--query",
        action="store",
        help="Overwrite an unsaved dataset",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "paths",
        metavar="path",
        nargs="*",
        help="Storage location, data object identifier, or dataset",
    ).complete = shtab.FILE
    parser.set_defaults(func=ls_command)

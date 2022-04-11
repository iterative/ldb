import argparse
from argparse import ArgumentParser

from ldb import __version__
from ldb.command import (
    add,
    add_storage,
    commit,
    completion,
    delete,
    diff,
    ds,
    evaluate,
    index,
    init,
    instantiate,
    ls,
    pull,
    stage,
    status,
    sync,
    tag,
)


def get_main_parser() -> ArgumentParser:
    parent_parser = argparse.ArgumentParser(add_help=False)
    verbosity_group = parent_parser.add_mutually_exclusive_group()
    verbosity_group.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        default=False,
        help="Quiet mode",
    )
    verbosity_group.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity",
    )
    parents = [parent_parser]

    main_parser = argparse.ArgumentParser(
        prog="ldb",
        description="Label Database",
        parents=parents,
    )
    main_parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    subparsers = main_parser.add_subparsers()
    add.add_parser(subparsers, parents)
    add_storage.add_parser(subparsers, parents)
    completion.add_parser(subparsers, parents)
    commit.add_parser(subparsers, parents)
    delete.add_parser(subparsers, parents)
    diff.add_parser(subparsers, parents)
    ds.add_parser(subparsers, parents)
    evaluate.add_parser(subparsers, parents)
    index.add_parser(subparsers, parents)
    init.add_parser(subparsers, parents)
    instantiate.add_parser(subparsers, parents)
    ls.add_parser(subparsers, parents)
    pull.add_parser(subparsers, parents)
    stage.add_parser(subparsers, parents)
    status.add_parser(subparsers, parents)
    sync.add_parser(subparsers, parents)
    tag.add_parser(subparsers, parents)
    return main_parser

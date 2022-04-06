from argparse import ArgumentParser, Namespace, _SubParsersAction
from typing import Iterable

from ldb.core import get_ldb_instance
from ldb.ds import ds, print_ds_listings


def ds_command(options: Namespace) -> None:  # pylint: disable=unused-argument
    ldb_dir = get_ldb_instance()
    ds_listings = ds(
        ldb_dir,
    )
    print_ds_listings(ds_listings)


def add_parser(
    subparsers: _SubParsersAction[ArgumentParser],
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "ds",
        parents=parents,
        help="List datasets",
    )
    parser.set_defaults(func=ds_command)

from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Iterable

from ldb.core import get_ldb_instance
from ldb.ds import delete_datasets, ds, print_ds_listings

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def ds_list_command(
    options: Namespace,  # pylint: disable=unused-argument
) -> None:
    ldb_dir = get_ldb_instance()
    ds_listings = ds(
        ldb_dir,
    )
    num_listings = print_ds_listings(ds_listings)
    if not num_listings:
        print("No saved datasets")


def ds_del_command(
    options: Namespace,
) -> None:  # pylint: disable=unused-argument
    delete_datasets(get_ldb_instance(), options.datasets)


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "ds",
        parents=parents,
        help="Manage saved datasets",
    )
    ds_subparsers = parser.add_subparsers(
        dest="subcommand",
        required=True,
    )

    ds_list_parser = ds_subparsers.add_parser(
        "list",
        parents=parents,
        help="List saved datasets",
    )
    ds_list_parser.set_defaults(func=ds_list_command)

    ds_del_parser = ds_subparsers.add_parser(
        "del",
        parents=parents,
        help="Delete datasets",
    )
    ds_del_parser.add_argument(
        dest="datasets",
        metavar="<dataset>",
        nargs="+",
    )
    ds_del_parser.set_defaults(func=ds_del_command)

import argparse
from typing import Iterable


def init(_options):
    pass


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "init",
        parents=parents,
        help="Initialize an empty ldb instance",
    )
    parser.set_defaults(func=init)

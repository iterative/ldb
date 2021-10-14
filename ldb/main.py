import argparse
import sys
from typing import List

from ldb import __version__
from ldb.command import init


def main(argv: List[str] = None):
    parent_parser = argparse.ArgumentParser(add_help=False)
    main_parser = argparse.ArgumentParser(
        prog="ldb",
        description="Label Database",
    )
    main_parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    subparsers = main_parser.add_subparsers()
    parents = [parent_parser]
    init.add_parser(subparsers, parents)
    options = main_parser.parse_args(args=argv)
    try:
        func = options.func
    except AttributeError:
        main_parser.print_usage()
        sys.exit(1)
    return func(options)

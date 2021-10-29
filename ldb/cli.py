import argparse

from ldb import __version__
from ldb.command import add_storage, completion, index, init


def get_main_parser():
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
    add_storage.add_parser(subparsers, parents)
    completion.add_parser(subparsers, parents)
    index.add_parser(subparsers, parents)
    init.add_parser(subparsers, parents)
    return main_parser

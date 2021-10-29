__all__ = ["main"]

import traceback
from typing import List

from ldb.cli import get_main_parser


def main(argv: List[str] = None):
    main_parser = get_main_parser()
    options = main_parser.parse_args(args=argv)
    try:
        func = options.func
    except AttributeError:
        main_parser.print_usage()
        return 1
    try:
        func(options)
    except Exception:  # pylint: disable=broad-except
        traceback.print_exc(limit=12)
        return 1
    return 0

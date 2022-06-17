__all__ = ["main"]

import sys
import traceback
from typing import List, Optional

from ldb.cli import get_main_parser
from ldb.exceptions import LDBException

MIN_PYTHON_VERSION_INFO = (3, 7)
MIN_PYTHON_VERSION = ".".join(map(str, MIN_PYTHON_VERSION_INFO))


def main(argv: Optional[List[str]] = None) -> int:
    main_parser = get_main_parser()
    options = main_parser.parse_args(args=argv)
    try:
        if sys.version_info < MIN_PYTHON_VERSION_INFO:
            raise ImportError(
                f"Python {MIN_PYTHON_VERSION} or greater is required. "
                f"Unsupported version running:\n{sys.version}",
            )
        try:
            func = options.func
        except AttributeError:
            main_parser.print_usage()
            return 1
        func(options)
    except Exception as exc:  # pylint: disable=broad-except
        return handle_exception(exc, options.verbose)
    return 0


def handle_exception(exception: BaseException, verbose: int = 0) -> int:
    if isinstance(exception, LDBException):
        print("ERROR:", exception, file=sys.stderr)
    else:
        print(
            "ERROR:",
            *traceback.format_exception_only(type(exception), exception),
            end="",
            file=sys.stderr,
        )
    if verbose > 1:
        traceback.print_exception(
            type(exception),
            exception,
            exception.__traceback__,
            file=sys.stderr,
        )
    return 1

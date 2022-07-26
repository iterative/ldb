__all__ = ["main"]

import sys
import warnings
from typing import List, Optional

from ldb.cli import get_main_parser, handle_exception
from ldb.warnings import SimpleWarningHandler

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
            main_parser.print_help()
            return 1
        with warnings.catch_warnings():
            warnings.showwarning = SimpleWarningHandler.showwarning
            func(options)
    except Exception as exc:  # pylint: disable=broad-except
        return handle_exception(exc, options.verbose)
    return 0

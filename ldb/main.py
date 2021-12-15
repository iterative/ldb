__all__ = ["main"]

import logging
import sys
from logging import Logger
from types import TracebackType
from typing import List, Optional, Tuple, Type, Union

from ldb.cli import get_main_parser

logger = logging.getLogger(__name__)

MIN_PYTHON_VERSION_INFO = (3, 7)
MIN_PYTHON_VERSION = ".".join(map(str, MIN_PYTHON_VERSION_INFO))


class QuietFormatter(logging.Formatter):
    def formatException(
        self,
        ei: Union[
            Tuple[Type[BaseException], BaseException, Optional[TracebackType]],
            Tuple[None, None, None],
        ],
    ) -> str:
        return ""


def configure_logger(log: Logger, verbose: bool) -> None:
    log = logging.getLogger("ldb")
    handler = logging.StreamHandler()
    fmt = "%(levelname)s: %(message)s"
    if verbose:
        formatter = logging.Formatter(fmt)
    else:
        formatter = QuietFormatter(fmt)
    handler.setFormatter(formatter)
    log.addHandler(handler)


def main(argv: Optional[List[str]] = None) -> int:
    main_parser = get_main_parser()
    options = main_parser.parse_args(args=argv)
    configure_logger(logger, options.verbose > 0)
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
        logger.exception(exc)
        return 1
    return 0

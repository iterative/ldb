import sys
import traceback
from argparse import ArgumentParser, Namespace
from gettext import gettext
from typing import Any, List, NoReturn, Optional, Sequence, Tuple, TypeVar

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
    get,
    index,
    init,
    instantiate,
    ls,
    pull,
    stage,
    status,
    sync,
    tag,
    transform,
)
from ldb.exceptions import LDBException
from ldb.params import InvalidParamError
from ldb.utils import print_error

ArgumentParserT = TypeVar("ArgumentParserT", bound=ArgumentParser)


class LDBArgumentParser(ArgumentParser):
    def __init__(self, **kwargs: Any) -> None:
        self.last_used_parser: Optional[ArgumentParser] = None
        super().__init__(**kwargs)

    def error(self, message: str) -> NoReturn:
        parser = (
            self if self.last_used_parser is None else self.last_used_parser
        )
        help_message = parser.format_help()
        args = {"prog": parser.prog, "message": message}
        message = gettext("%(prog)s: error: %(message)s\n") % args
        print_error(f"{message}\n{help_message}", end="")
        self.exit(2)

    def parse_known_args(
        self,
        args: Optional[Sequence[str]] = None,
        namespace: Optional[Namespace] = None,
    ) -> Tuple[Namespace, List[str]]:
        namespace, args = super().parse_known_args(args, namespace)

        # cache the final subcommand's parser
        if not hasattr(namespace, "parser"):
            namespace.parser = self
        self.last_used_parser = namespace.parser
        return namespace, args


def get_main_parser() -> ArgumentParser:
    parent_parser = LDBArgumentParser(add_help=False)
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

    main_parser = LDBArgumentParser(
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
    subparsers = main_parser.add_subparsers(
        title="subcommands",
        description="valid subcommands",
        metavar="<command>",
        dest="command",
        help="Use `ldb <command> -h` for command-specific help.",
        required=True,
    )
    add.add_parser(subparsers, parents)
    add_storage.add_parser(subparsers, parents)
    completion.add_parser(subparsers, parents)
    commit.add_parser(subparsers, parents)
    delete.add_parser(subparsers, parents)
    diff.add_parser(subparsers, parents)
    ds.add_parser(subparsers, parents)
    evaluate.add_parser(subparsers, parents)
    get.add_parser(subparsers, parents)
    index.add_parser(subparsers, parents)
    init.add_parser(subparsers, parents)
    instantiate.add_parser(subparsers, parents)
    ls.add_parser(subparsers, parents)
    pull.add_parser(subparsers, parents)
    stage.add_parser(subparsers, parents)
    status.add_parser(subparsers, parents)
    sync.add_parser(subparsers, parents)
    tag.add_parser(subparsers, parents)
    transform.add_parser(subparsers, parents)
    return main_parser


def handle_exception(exception: BaseException, verbose: int = 0) -> int:
    if isinstance(exception, (LDBException, InvalidParamError)):
        print_error(exception)
    else:
        print_error(
            *traceback.format_exception_only(type(exception), exception),
            end="",
        )
    if verbose > 1:
        traceback.print_exception(
            type(exception),
            exception,
            exception.__traceback__,
            file=sys.stderr,
        )
    return 1

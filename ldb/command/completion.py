from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Iterable

import shtab

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def completion_command(options: Namespace) -> None:
    from ldb.cli import (  # pylint: disable=import-outside-toplevel,cyclic-import # noqa: E501
        get_main_parser,
    )

    parser = get_main_parser()
    script = shtab.complete(parser, shell=options.shell)
    print(script)


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "completion",
        parents=parents,
        help="Output shell completion script",
    )
    parser.add_argument(
        "-s",
        "--shell",
        help="Shell syntax for completions.",
        default="bash",
        choices=shtab.SUPPORTED_SHELLS,
    )
    parser.set_defaults(func=completion_command)

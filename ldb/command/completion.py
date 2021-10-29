import argparse
from typing import Iterable

import shtab


def completion_command(options):
    from ldb.cli import (  # pylint: disable=import-outside-toplevel
        get_main_parser,
    )

    parser = get_main_parser()
    script = shtab.complete(parser, shell=options.shell)
    print(script)


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
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
        choices=["bash", "zsh"],
    )
    parser.set_defaults(func=completion_command)

import argparse
from typing import Iterable

import shtab


def choice_str(choices: Iterable[str]) -> str:
    choice_strings = ",".join(str(c) for c in choices)
    return f"{{{choice_strings}}}"


def add_data_object_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--query",
        metavar="<query>",
        dest="annotation_query",
        help="JMESPath query applied to annotations",
    )
    parser.add_argument(
        "--file",
        metavar="<query>",
        dest="file_query",
        help="JMESPath query applied to file attributes",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "paths",
        metavar="<path>",
        nargs="*",
        help="Storage location, data object identifier, or dataset",
    ).complete = shtab.FILE


def add_data_format_arguments(
    parser: argparse.ArgumentParser,
    default: str,
    formats: Iterable[str],
) -> None:
    parser.add_argument(
        "-m",
        "--format",
        default=default,
        metavar="<format>",
        choices=formats,
        help=(
            f"Data format to use. (default: {default}) "
            f"Options: {choice_str(formats)}"
        ),
    )

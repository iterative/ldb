import argparse
from argparse import ArgumentParser, Namespace
from typing import Any, Iterable, Optional, Sequence, Union

import shtab

from ldb.op_type import OpType


class AppendConstValuesAction(argparse.Action):
    def __call__(
        self,
        parser: ArgumentParser,
        namespace: Namespace,
        values: Union[str, Sequence[Any], None],
        option_string: Optional[str] = None,
    ) -> None:
        items = getattr(namespace, self.dest, None) or []
        items.append((self.const, values))
        setattr(namespace, self.dest, items)


def choice_str(choices: Iterable[str]) -> str:
    choice_strings = ",".join(str(c) for c in choices)
    return f"{{{choice_strings}}}"


def add_data_object_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--query",
        metavar="<query>",
        const=OpType.ANNOTATION_QUERY,
        default=[],
        dest="query_args",
        action=AppendConstValuesAction,
        help="JMESPath query applied to annotations",
    )
    parser.add_argument(
        "--file",
        metavar="<query>",
        const=OpType.FILE_QUERY,
        default=[],
        dest="query_args",
        action=AppendConstValuesAction,
        help="JMESPath query applied to file attributes",
    )
    parser.add_argument(
        "--limit",
        metavar="<num>",
        const=OpType.LIMIT,
        default=[],
        dest="query_args",
        action=AppendConstValuesAction,
        help="Take the first num items",
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

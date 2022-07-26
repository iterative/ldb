import json
import re
from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Iterable, Union

from ldb.cli_utils import add_data_obj_params
from ldb.core import get_ldb_instance
from ldb.evaluate import evaluate
from ldb.utils import DATA_OBJ_ID_PREFIX

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def evaluate_command(options: Namespace) -> None:
    indent = get_indent_value(options.indent)
    for data_object_hash, *results in evaluate(
        get_ldb_instance(),
        options.paths,
        options.query_args,
        warn=True,
    ):
        if not options.json_only:
            print(f"{DATA_OBJ_ID_PREFIX}{data_object_hash}")
        for item in results:
            print(json.dumps(item, indent=indent))
        if not options.json_only:
            print()


def get_indent_value(indent: str) -> Union[str, int, None]:
    if indent == "none":
        return None
    if re.search(r"^\s*$", indent):
        return indent
    if re.search(r"^\d+$", indent):
        return int(indent)
    raise ValueError(f"Invalid indent string: {indent!r}")


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "eval",
        parents=parents,
        help="Evaluate a query on the specified annotations",
    )
    parser.add_argument(
        "-j",
        "--json-only",
        action="store_true",
        default=False,
        help="Show JSON output only instead of showing object hashes",
    )
    parser.add_argument(
        "--indent",
        metavar="<value>",
        default="2",
        type=str,
        help=(
            "Indentation for JSON output as a whitespace string or an "
            "integer specifying the number of spaces"
        ),
    )
    add_data_obj_params(parser, dest="query_args")
    parser.set_defaults(func=evaluate_command)

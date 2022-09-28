import json
from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Iterable

from ldb.cli_utils import (
    AppendConstValuesAction,
    add_data_obj_params,
    get_indent_value,
)
from ldb.core import get_ldb_instance
from ldb.evaluate import evaluate
from ldb.op_type import OpType
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
        options.show_args,
        warn=True,
    ):
        if not options.json_only:
            print(f"{DATA_OBJ_ID_PREFIX}{data_object_hash}")
        for item in results:
            print(json.dumps(item, indent=indent))
        if not options.json_only:
            print()


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
    parser.add_argument(
        "--show",
        metavar="<query>",
        const=OpType.ANNOTATION_QUERY,
        default=[],
        dest="show_args",
        action=AppendConstValuesAction,
        help=(
            "JMESPath-like query applied to annotations and shown as results"
        ),
    )
    parser.add_argument(
        "--jshow",
        metavar="<query>",
        const=OpType.JP_ANNOTATION_QUERY,
        default=[],
        dest="show_args",
        action=AppendConstValuesAction,
        help=(
            "Fully compliant JMESPath query applied to annotations "
            "and shown as results"
        ),
    )
    parser.add_argument(
        "--file-show",
        metavar="<query>",
        const=OpType.FILE_QUERY,
        default=[],
        dest="show_args",
        action=AppendConstValuesAction,
        help=(
            "JMESPath-like query applied to data object file attributes "
            "and shown as results"
        ),
    )
    add_data_obj_params(parser, dest="query_args")
    parser.set_defaults(func=evaluate_command)

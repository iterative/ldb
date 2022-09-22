import json
from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Iterable

from ldb.cli_utils import AppendConstValuesAction, get_indent_value
from ldb.op_type import OpType
from ldb.query_cmd import query

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def query_command(options: Namespace) -> None:
    indent = get_indent_value(options.indent)
    for result in query(
        options.paths,
        options.query_args,
        options.show_args,
        slurp=options.slurp,
        unslurp=options.unslurp,
        warn=True,
    ):
        for item in result:
            print(json.dumps(item, indent=indent))


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "query",
        parents=parents,
        help="Evaluate a query on the supplied JSON files",
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
        "--query",
        metavar="<query>",
        const=OpType.ANNOTATION_QUERY,
        default=[],
        dest="query_args",
        action=AppendConstValuesAction,
        help=(
            "JMESPath-like query applied to the supplied JSON files "
            "as a filter for matching files"
        ),
    )
    parser.add_argument(
        "--jquery",
        metavar="<query>",
        const=OpType.JP_ANNOTATION_QUERY,
        default=[],
        dest="query_args",
        action=AppendConstValuesAction,
        help=(
            "Fully compliant JMESPath query applied to the "
            "supplied JSON files as a filter for matching files"
        ),
    )
    parser.add_argument(
        "--show",
        metavar="<query to show>",
        const=OpType.ANNOTATION_QUERY,
        default=[],
        dest="show_args",
        action=AppendConstValuesAction,
        help=(
            "JMESPath-like query applied to the supplied "
            "JSON files and shown as results"
        ),
    )
    parser.add_argument(
        "--jshow",
        metavar="<query to show>",
        const=OpType.JP_ANNOTATION_QUERY,
        default=[],
        dest="show_args",
        action=AppendConstValuesAction,
        help=(
            "Fully compliant JMESPath query applied to to the "
            "supplied JSON files and shown as results"
        ),
    )
    parser.add_argument(
        "-s",
        "--slurp",
        default=False,
        action="store_true",
        help=(
            "Place multiple files or objects in a top-level array "
            "before running the query"
        ),
    )
    parser.add_argument(
        "-u",
        "--unslurp",
        default=False,
        action="store_true",
        help=(
            "Split a top-level array for each file into "
            "multiple JSON objects before running the query"
        ),
    )
    parser.add_argument(
        "paths",
        metavar="<file_path>",
        default=[],
        nargs="*",
        help=(
            "JSON file paths which to run the query on "
            "(will read from stdin if no files are specified)"
        ),
    )
    parser.set_defaults(func=query_command)

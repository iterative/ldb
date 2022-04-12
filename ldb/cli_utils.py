from argparse import Action, ArgumentParser, Namespace
from typing import Any, Iterable, Optional, Sequence, Union

import shtab

from ldb.op_type import OpType


class AppendConstValuesAction(Action):
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


def add_data_obj_params(parser: ArgumentParser, dest: str) -> None:
    add_base_data_object_options(parser, dest)
    add_data_object_paths(parser)


def add_base_data_object_options(parser: ArgumentParser, dest: str) -> None:
    parser.add_argument(
        "--query",
        metavar="<query>",
        const=OpType.ANNOTATION_QUERY,
        default=[],
        dest=dest,
        action=AppendConstValuesAction,
        help="JMESPath query applied to annotations",
    )
    parser.add_argument(
        "--file",
        metavar="<query>",
        const=OpType.FILE_QUERY,
        default=[],
        dest=dest,
        action=AppendConstValuesAction,
        help="JMESPath query applied to data object file attributes",
    )
    parser.add_argument(
        "--tag",
        metavar="<tag>",
        const=OpType.TAG_QUERY,
        default=[],
        dest=dest,
        action=AppendConstValuesAction,
        help="Select only data objects that contain this tag",
    )
    parser.add_argument(
        "--no-tag",
        metavar="<tag>",
        const=OpType.NO_TAG_QUERY,
        default=[],
        dest=dest,
        action=AppendConstValuesAction,
        help="Select only data objects that do not contain this tag",
    )
    parser.add_argument(
        "--limit",
        metavar="<n>",
        const=OpType.LIMIT,
        default=[],
        dest=dest,
        action=AppendConstValuesAction,
        type=int,
        help="Take the first n items",
    )
    parser.add_argument(
        "--sample",
        metavar="<probability>",
        const=OpType.SAMPLE,
        default=[],
        dest=dest,
        action=AppendConstValuesAction,
        type=float,
        help=(
            "Each item will have the given probability of being selected. "
            "Must be a float between 0.0 and 1.0 inclusive"
        ),
    )
    parser.add_argument(
        "--pipe",
        nargs="+",
        metavar="<exec>",
        const=OpType.PIPE,
        default=[],
        dest=dest,
        action=AppendConstValuesAction,
        help="Executable to filter or sort data objects",
    )


def add_data_object_paths(parser: ArgumentParser) -> None:
    parser.add_argument(  # type: ignore[attr-defined]
        "paths",
        metavar="<path>",
        nargs="*",
        help="Storage location, data object identifier, or dataset",
    ).complete = shtab.FILE


def add_data_format_arguments(
    parser: ArgumentParser,
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

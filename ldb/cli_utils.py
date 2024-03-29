import copy
import re
from argparse import Action, ArgumentParser, Namespace
from typing import Any, Iterable, List, Optional, Sequence, Tuple, Union

import shtab
from tomlkit import document
from tomlkit.toml_document import TOMLDocument

from ldb import config
from ldb.data_formats import INSTANTIATE_FORMATS, Format
from ldb.op_type import OpType

PARAM_KEY_PATTERN = r"^[a-zA-Z][a-zA-Z0-9]*(-[a-zA-Z0-9]+)*$"
SIMPLE_NAME_PATTERN = r"^[^\s,]+$"


def json_bool(text: str) -> bool:
    if text == "true":
        return True
    if text == "false":
        return False
    raise ValueError(f"Not a JSON bool: {text}")


def simple_name_list(value: str) -> List[str]:
    result = []
    for item in value.split(","):
        item = item.strip()
        item_match = re.search(SIMPLE_NAME_PATTERN, item)
        if item_match is None:
            raise ValueError(item)
        result.append(item_match.group())
    return result


def ws_path_identifier(ws_path: str) -> str:
    if ws_path.startswith("ws:"):
        return ws_path[3:]
    raise ValueError("workspace path identifier must begin with 'ws:'")


def get_indent_value(indent: str) -> Union[str, int, None]:
    if indent == "none":
        return None
    if re.search(r"^\s*$", indent):
        return indent
    if re.search(r"^\d+$", indent):
        return int(indent)
    raise ValueError(f"Invalid indent string: {indent!r}")


class ExtendAction(Action):
    def __call__(
        self,
        parser: ArgumentParser,
        namespace: Namespace,
        values: Union[str, Sequence[Any], None],
        option_string: Optional[str] = None,
    ) -> None:
        if values is None:
            raise ValueError("values must be iterable")
        items = getattr(namespace, self.dest, None) or []
        items = copy.copy(items)
        items.extend(values)
        setattr(namespace, self.dest, items)


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


def add_physical_workflow_arguments(parser: ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--physical",
        action="store_const",
        const="physical",
        dest="physical",
        help="Use physical workflow to auto-instantiate/delete data objects and annotations",
    )
    group.add_argument(
        "--logical",
        action="store_const",
        const="logical",
        dest="physical",
        help="Use logical workflow without auto-instantiating/deleting objects or annotations",
    )
    add_data_format_arguments(
        parser,
        default="from config or bare-pairs",
        formats=INSTANTIATE_FORMATS,
    )
    add_param_option(parser)


def using_physical_workflow(physical: Optional[str]) -> bool:
    if physical == "physical":
        return True
    if physical == "logical":
        return False
    cfg: TOMLDocument = config.load_first() or document()
    return bool(cfg.get("core", {}).get("physical_workflow", False))


def physical_workflow_format(fmt: Optional[str]) -> str:
    if fmt is not None and fmt != "from config or bare-pairs":
        return fmt
    cfg: TOMLDocument = config.load_first() or document()
    return str(cfg.get("core", {}).get("default_physical_format", Format.BARE))


def add_data_obj_params(
    parser: ArgumentParser,
    dest: str,
) -> None:
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
        help="JMESPath-like query applied to annotations",
    )
    parser.add_argument(
        "--jquery",
        metavar="<query>",
        const=OpType.JP_ANNOTATION_QUERY,
        default=[],
        dest=dest,
        action=AppendConstValuesAction,
        help="Fully compliant JMESPath query applied to annotations",
    )
    parser.add_argument(
        "--file",
        metavar="<query>",
        const=OpType.FILE_QUERY,
        default=[],
        dest=dest,
        action=AppendConstValuesAction,
        help="JMESPath-like query applied to data object file attributes",
    )
    parser.add_argument(
        "--path",
        metavar="<pattern>",
        const=OpType.PATH_QUERY,
        default=[],
        dest=dest,
        action=AppendConstValuesAction,
        help="Python regular expression which must match one of the object's indexing paths",
    )
    parser.add_argument(
        "--tag",
        metavar="<tag>",
        const=OpType.TAG_QUERY,
        default=[],
        dest=dest,
        type=simple_name_list,
        action=AppendConstValuesAction,
        help=(
            "Comma-separated list of tags. Select only data objects that "
            "contain at least one."
        ),
    )
    parser.add_argument(
        "--no-tag",
        metavar="<tag>",
        const=OpType.NO_TAG_QUERY,
        default=[],
        dest=dest,
        type=simple_name_list,
        action=AppendConstValuesAction,
        help=(
            "Comma-separated list of tags. Select only data objects where at "
            "least one of these tags is missing."
        ),
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
        "--shuffle",
        nargs=0,
        const=OpType.SHUFFLE,
        default=[],
        dest=dest,
        action=AppendConstValuesAction,
        help="Shuffle items randomly",
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
        help=f"Data format to use. (default: {default}) Options: {choice_str(formats)}",
    )


def add_target_dir_argument(parser: ArgumentParser) -> None:
    parser.add_argument(  # type: ignore[attr-defined]
        "-t",
        "--target",
        dest="target_dir",
        default=".",
        metavar="<dir>",
        help="Target directory",
    ).complete = shtab.DIR


def add_instantiate_arguments(
    parser: ArgumentParser,
    include_force: bool = True,
) -> None:
    add_data_obj_params(parser, dest="query_args")
    add_data_format_arguments(
        parser,
        default=Format.BARE,
        formats=INSTANTIATE_FORMATS,
    )
    if include_force:
        parser.add_argument(
            "-f",
            "--force",
            action="store_true",
            default=False,
            help="Remove existing workspace contents",
        )
    add_target_dir_argument(parser)
    parser.add_argument(
        "--apply",
        nargs="+",
        metavar="<exec>",
        default=None,
        dest="apply",
        help="Executable to apply to data objects and annotations",
    )
    add_param_option(parser)


def param(option_str: str) -> Tuple[str, str]:
    key, value = option_str.split("=", 1)
    validate_param_key(key)
    return key, value


def validate_param_key(key: str) -> None:
    if not re.search(PARAM_KEY_PATTERN, key):
        raise ValueError(
            f"Invalid param key {key!r}, must match pattern, {PARAM_KEY_PATTERN!r}"
        )


def add_param_option(parser: ArgumentParser) -> None:
    parser.add_argument(
        "-p",
        "--param",
        metavar="<key>=<value>",
        default=[],
        action="append",
        dest="params",
        type=param,
        help="Format-specific option. May be used multiple times",
    )

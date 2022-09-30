import sys
from functools import partial
from json import load
from typing import Iterable, List, Optional, Sequence, TextIO, Tuple

from ldb.func_utils import apply_optional
from ldb.op_type import OpType
from ldb.query.search import (
    BoolSearchFunc,
    SearchFunc,
    get_bool_search_func,
    get_search_func,
)
from ldb.typing import JSONDecoded


def run_query_filters(
    objects: List[JSONDecoded],
    filter_functions: Iterable[BoolSearchFunc],
) -> List[JSONDecoded]:
    for filter_func in filter_functions:
        objects = [obj for obj, keep in zip(objects, filter_func(objects)) if keep]
    return objects


def query_ops_to_filters(
    query_ops: Iterable[Tuple[str, str]],
    warn: bool = True,
) -> List[BoolSearchFunc]:
    filters: List[BoolSearchFunc] = []
    for op_type, arg in query_ops:
        if op_type == OpType.ANNOTATION_QUERY:
            assert isinstance(arg, str)
            filters.append(get_bool_search_func(arg, True, warn=warn))
        elif op_type == OpType.JP_ANNOTATION_QUERY:
            assert isinstance(arg, str)
            filters.append(get_bool_search_func(arg, False, warn=warn))
    return filters


def show_ops_to_searches(
    show_args: Iterable[Tuple[str, str]],
    warn: bool = True,
) -> List[Optional[SearchFunc]]:
    searches: List[Optional[SearchFunc]] = []
    for op_type, arg in show_args:
        if op_type == OpType.ANNOTATION_QUERY:
            searches.append(
                apply_optional(
                    partial(get_search_func, use_custom=True, warn=warn),
                    arg,
                ),
            )
        elif op_type == OpType.JP_ANNOTATION_QUERY:
            searches.append(
                apply_optional(
                    partial(get_search_func, use_custom=False, warn=warn),
                    arg,
                ),
            )
    return searches


def load_json_with_unslurp(
    file_obj: TextIO,
    objects: List[JSONDecoded],
    unslurp: bool = False,
) -> None:
    json_data: JSONDecoded = load(file_obj)
    if unslurp and isinstance(json_data, list):
        # This is for multiple objects passed in as a top-level array to stdin
        # or if each file has a top-level array to expand
        objects.extend(json_data)
    else:
        objects.append(json_data)


def query(
    paths: Sequence[str],
    query_ops: Iterable[Tuple[str, str]],
    show_ops: Iterable[Tuple[str, str]],
    slurp: bool = False,
    unslurp: bool = False,
    warn: bool = True,
) -> Iterable[Iterable[JSONDecoded]]:
    filters = query_ops_to_filters(query_ops, warn)
    searches = show_ops_to_searches(show_ops, warn)

    objects: List[JSONDecoded] = []
    if not paths:
        if sys.stdin.isatty():
            raise RuntimeError(
                "Please specify JSON files for this query, or pipe in JSON input to stdin",
            )
        # Load from stdin
        load_json_with_unslurp(sys.stdin, objects, unslurp=unslurp)
    else:
        for path in paths:
            # Load from files
            with open(path, encoding="utf-8") as fp:
                load_json_with_unslurp(fp, objects, unslurp=unslurp)

    if slurp:
        # Combine all objects into a single array, and run filters once
        objects = [objects]

    if filters:
        # Run query filters on files
        objects = run_query_filters(objects, filters)

    results: Iterable[Iterable[JSONDecoded]]

    if not searches:
        for obj in objects:
            yield (obj,)
    else:
        # This interleaves results so that all search results for
        # a given object are grouped together
        results = zip(*(search(objects) for search in searches if search))

        yield from results

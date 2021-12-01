from typing import Any, Callable, Iterable, Iterator

import jmespath
from jmespath.exceptions import ParseError

SearchFunc = Callable[[Iterable[Any]], Iterator[Any]]
BoolSearchFunc = Callable[[Iterable[Any]], Iterator[bool]]


def get_search_func(
    query_str: str,
) -> SearchFunc:
    """Compile `query_str` and return a search function."""
    query_obj = jmespath.compile(query_str)

    def search(objects: Iterable[Any]) -> Iterator[Any]:
        for obj in objects:
            yield query_obj.search(obj)

    return search


def get_bool_search_func(
    query_str: str,
) -> BoolSearchFunc:
    """
    Adapt `query_str` to cast result to a bool and return a search function.

    This uses JMESPath's boolean rules, not Python's, so the following
    evaluate to false:

        Empty list: []
        Empty object: {}
        Empty string: ""
        False boolean: false
        Null value: null

    See https://jmespath.org/specification.html#or-expressions
    """
    try:
        query_obj = jmespath.compile(f"{query_str} && `true` || `false`")
    except ParseError:
        # If the expression above raises a ParseError, this should too
        # This way we show the original query string in the error message
        jmespath.compile(query_str)
        raise

    def search(objects: Iterable[Any]) -> Iterator[bool]:
        for obj in objects:
            yield query_obj.search(obj)

    return search

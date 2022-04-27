import re
import warnings
from typing import Callable, Collection, Iterable, Iterator

import jmespath
from jmespath.exceptions import JMESPathError, ParseError

from ldb.jmespath import custom_compile, jp_compile
from ldb.query.utils import OptionsCache
from ldb.typing import JSONDecoded
from ldb.warnings import SimpleWarningHandler

SearchFunc = Callable[[Iterable[JSONDecoded]], Iterator[JSONDecoded]]
BoolSearchFunc = Callable[[Iterable[JSONDecoded]], Iterator[bool]]

OPTIONS_CACHE = OptionsCache()


def get_search_func(
    query_str: str,
    use_custom: bool = True,
    warn: bool = True,
) -> SearchFunc:
    """
    Compile `query_str` and return a search function.
    """
    if use_custom:
        query_obj = custom_compile(query_str)
    else:
        query_obj = jp_compile(query_str)

    runtime_exc_types = set()

    def search(objects: Iterable[JSONDecoded]) -> Iterator[JSONDecoded]:
        with warnings.catch_warnings():
            warnings.showwarning = SimpleWarningHandler.showwarning
            for obj in objects:
                try:
                    yield query_obj.search(obj, options=OPTIONS_CACHE.get())
                except JMESPathError as exc:
                    if warn:
                        exc_type = type(exc)
                        if exc_type not in runtime_exc_types:
                            warnings.warn(
                                f"{exc_type.__name__}: {exc}",
                                RuntimeWarning,
                                stacklevel=2,
                            )
                        runtime_exc_types.add(exc_type)
                    yield None

    return search


def get_bool_search_func(
    query_str: str,
    use_custom: bool = True,
    warn: bool = True,
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
    modified_query_str = f"{query_str} && `true` || `false`"
    try:
        if use_custom:
            query_obj = custom_compile(modified_query_str)
        else:
            query_obj = jp_compile(modified_query_str)
    except ParseError:
        # If the expression above raises a ParseError, this should too
        # This way we show the original query string in the error message
        jmespath.compile(query_str)
        raise

    runtime_exc_types = set()

    def search(objects: Iterable[JSONDecoded]) -> Iterator[bool]:
        with warnings.catch_warnings():
            warnings.showwarning = SimpleWarningHandler.showwarning
            for obj in objects:
                try:
                    yield query_obj.search(  # type: ignore[misc]
                        obj,
                        options=OPTIONS_CACHE.get(),
                    )
                except JMESPathError as exc:
                    if warn:
                        exc_type = type(exc)
                        if exc_type not in runtime_exc_types:
                            warnings.warn(
                                f"{exc_type.__name__}: {exc}",
                                RuntimeWarning,
                                stacklevel=2,
                            )
                        runtime_exc_types.add(exc_type)
                    yield False

    return search


def get_tag_func(tags: Collection[str]) -> BoolSearchFunc:
    def search(objects: Iterable[Collection[str]]) -> Iterator[bool]:
        for obj in objects:
            yield any(t in obj for t in tags)

    return search  # type: ignore[return-value]


def get_no_tag_func(tags: Collection[str]) -> BoolSearchFunc:
    def search(objects: Iterable[Collection[str]]) -> Iterator[bool]:
        for obj in objects:
            yield any(t not in obj for t in tags)

    return search  # type: ignore[return-value]


def get_path_func(pattern: str) -> BoolSearchFunc:
    re_obj = re.compile(pattern)

    def search(objects: Iterable[Collection[str]]) -> Iterator[bool]:
        for obj in objects:
            for path in obj:
                if re_obj.search(path) is not None:
                    yield True
                    break
            else:
                yield False

    return search  # type: ignore[return-value]

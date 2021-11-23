from typing import Any, Generator, Iterable

import jmespath


def query(
    query_str: str,
    objects: Iterable[Any],
) -> Generator[Any, None, None]:
    """For each item in `objects`, apply `query_str`."""
    query_obj = jmespath.compile(query_str)
    for obj in objects:
        yield query_obj.search(obj)

from typing import Any, Callable, Iterable, Iterator

import jmespath


def get_search_func(
    query_str: str,
) -> Callable[[Iterable[Any]], Iterator[Any]]:
    """Compile `query_str` and return a search function."""
    query_obj = jmespath.compile(query_str)

    def search(objects: Iterable[Any]) -> Iterator[Any]:
        for obj in objects:
            yield query_obj.search(obj)

    return search

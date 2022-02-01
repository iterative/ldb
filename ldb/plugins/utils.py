from typing import Any, Iterable, Iterator, TypeVar

T = TypeVar("T")


def sort_by_iterable(
    data: Iterable[T],
    iterable: Iterable[Any],
) -> Iterator[T]:
    for _, d in sorted(
        zip(iterable, data),
        key=lambda x: x[0],  # type: ignore[no-any-return]
        reverse=True,
    ):
        yield d

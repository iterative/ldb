from typing import Any, Callable, Generic, Type, TypeVar, overload

_T = TypeVar("_T")

class cached_property(Generic[_T]):
    fget: Callable[[Any], _T]
    def __init__(self, fget: Callable[[Any], _T]) -> None: ...
    @overload
    def __get__(self, instance: None, owner: Type[Any] | None = ...) -> cached_property[_T]: ...
    @overload
    def __get__(self, instance: object, owner: Type[Any] | None = ...) -> _T: ...

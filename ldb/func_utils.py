from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    TypeVar,
    Union,
    overload,
)

if TYPE_CHECKING:
    from _typeshed import SupportsGetItem


_KT_contra = TypeVar("_KT_contra", contravariant=True)
_VT_co = TypeVar("_VT_co", covariant=True)
_T = TypeVar("_T")


@overload
def apply_optional(func: Callable[[Any], Any], arg: None) -> None:
    ...


@overload
def apply_optional(func: Callable[[Any], Any], arg: Any) -> Any:
    ...


def apply_optional(func: Callable[[Any], Any], arg: Any) -> Any:
    if arg is None:
        return None
    return func(arg)


@overload
def get_first(
    container: "SupportsGetItem[_KT_contra, _VT_co]",
    *keys: _KT_contra,
) -> Optional[_VT_co]:
    ...


@overload
def get_first(
    container: "SupportsGetItem[_KT_contra, _VT_co]",
    *key: _KT_contra,
    default: _T,
) -> Union[_VT_co, _T]:
    ...


def get_first(
    container: "SupportsGetItem[_KT_contra, _VT_co]",
    *keys: _KT_contra,
    default: Optional[_T] = None,
) -> Union[_VT_co, Optional[_T]]:
    for key in keys:
        try:
            return container[key]
        except LookupError:
            pass
    return default

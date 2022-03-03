from typing import Any, Callable, overload


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

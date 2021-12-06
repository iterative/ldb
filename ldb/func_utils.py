from typing import Any, Callable


def apply_optional(func: Callable[[Any], Any], arg: Any) -> Any:
    if arg is None:
        return None
    return func(arg)

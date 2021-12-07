from typing import Any, Callable, Dict, Sequence, Tuple, Type, Union

from ldb.typing import JSONInstanceFunc

TYPES_MAP: Any
REVERSE_TYPES_MAP: Any

def signature(
    *arguments: Dict[str, Union[Sequence[str], bool]]
) -> Callable[..., JSONInstanceFunc]: ...

class FunctionRegistry(type):
    def __init__(
        cls, name: str, bases: Tuple[Type[Any], ...], attrs: Dict[str, Any]
    ) -> None: ...

class Functions:
    FUNCTION_TABLE: Any
    def call_function(
        self, function_name: str, resolved_args: Sequence[Any]
    ) -> Any: ...

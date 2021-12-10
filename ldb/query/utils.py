from dataclasses import dataclass
from functools import wraps
from typing import Any, Dict, List, Optional, Sequence, Type, Union

from jmespath import functions
from jmespath.visitor import Options

from ldb.config import get_ldb_dir
from ldb.core import is_ldb_instance
from ldb.query.functions import CUSTOM_FUNCTIONS
from ldb.query.user_functions import load_user_functions
from ldb.typing import (
    JMESPathValue,
    JSONArgTypes,
    JSONFunc,
    JSONFuncMapping,
    JSONFuncMutableMapping,
    JSONInstanceFunc,
)

VALID_ARG_TYPES = set(functions.REVERSE_TYPES_MAP)


def create_func(
    func: JSONFunc,
    arg_types: JSONArgTypes,
) -> JSONInstanceFunc:
    variadic = False
    if arg_types and arg_types[-1] == "*":
        variadic = True
        arg_types = arg_types[:-1]

    signature: List[Dict[str, Union[Sequence[str], bool]]] = []
    for arg in arg_types:
        types = arg.split("|")
        for type_str in types:
            validate_arg_type(type_str)
        signature.append({"types": types})

    if signature and variadic:
        signature[-1]["variadic"] = True

    @wraps(func)
    @functions.signature(*signature)
    def new_func(_self: Any, *args: JMESPathValue) -> JMESPathValue:
        return func(*args)

    return new_func


def validate_arg_type(arg_type: str) -> None:
    types = arg_type.split("-", 1)
    if len(types) == 2:
        if types[0] != "array":
            raise ValueError(
                f"Invalid arg type: {arg_type!r}\n"
                f"{types[0]!r} does not support sub-types. "
                "Only 'array' does.",
            )
        if types[1] not in VALID_ARG_TYPES:
            raise ValueError(
                f"Invalid arg type: {arg_type!r}\n"
                f"{types[1]!r} is not a valid sub-type.\n"
                f"Use one of: {VALID_ARG_TYPES}",
            )
    elif types[0] not in VALID_ARG_TYPES:
        raise ValueError(
            f"Invalid arg type: {arg_type!r}\n"
            f"Use one of: {VALID_ARG_TYPES}",
        )


def create_func_holder(custom_functions: JSONFuncMapping) -> Type[Any]:
    class FuncHolder:
        pass

    for name, (func, arg_types) in custom_functions.items():
        setattr(FuncHolder, "_func_" + name, create_func(func, arg_types))
    return FuncHolder


def create_custom_options(custom_functions: JSONFuncMapping) -> Options:
    FuncHolder = create_func_holder(custom_functions)

    class CustomFunctions(
        functions.Functions,
        FuncHolder,  # type: ignore[misc,valid-type]
    ):
        pass

    return Options(custom_functions=CustomFunctions())


def get_all_custom_functions() -> JSONFuncMutableMapping:
    custom_functions: JSONFuncMutableMapping = (
        CUSTOM_FUNCTIONS.copy()  # type: ignore[assignment]
    )
    ldb_dir = get_ldb_dir()
    if is_ldb_instance(ldb_dir):
        custom_functions.update(load_user_functions(ldb_dir, CUSTOM_FUNCTIONS))
    return custom_functions


@dataclass(init=False)
class OptionsCache:
    """
    Cache for lazy loading of options for JMESPath queries.

    User-defined custom functions may be any python file and importing
    could be expensive. A new instance of this class will not load user-
    defined custom functions until the first call to the `get` method.
    Then subsequent calls to `get` return the cached `Options` object.
    """

    _options: Optional[Options] = None

    def get(self) -> Options:
        if self._options is None:
            self._options = create_custom_options(get_all_custom_functions())
        return self._options

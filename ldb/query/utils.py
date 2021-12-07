from functools import wraps
from typing import Any, Dict, List, Mapping, Sequence, Tuple, Type, Union

from jmespath import functions
from jmespath.visitor import Options

from ldb.typing import JSONDecoded, JSONFunc, JSONInstanceFunc

JSONFuncArgTypes = Sequence[str]
CustomFunctionMapping = Mapping[str, Tuple[JSONFunc, JSONFuncArgTypes]]

VALID_ARG_TYPES = set(functions.REVERSE_TYPES_MAP)


def create_func(
    func: JSONFunc,
    arg_types: JSONFuncArgTypes,
) -> JSONInstanceFunc:
    variadic = False
    if arg_types and arg_types[-1] == "*":
        variadic = True
        arg_types = arg_types[:-1]

    signature: List[Dict[str, Union[Sequence[str], bool]]] = []
    for arg in arg_types:
        types = arg.split("|")
        for type_str in types:
            if type_str not in VALID_ARG_TYPES:
                raise ValueError(f"Invalid JMESPath arg type: {type_str}")
        signature.append({"types": types})

    if signature and variadic:
        signature[-1]["variadic"] = True

    @wraps(func)
    @functions.signature(*signature)
    def new_func(_self: Any, *args: JSONDecoded) -> JSONDecoded:
        return func(*args)

    return new_func


def create_func_holder(custom_functions: CustomFunctionMapping) -> Type[Any]:
    class FuncHolder:
        pass

    for name, (func, arg_types) in custom_functions.items():
        setattr(FuncHolder, "_func_" + name, create_func(func, arg_types))
    return FuncHolder


def create_custom_options(custom_functions: CustomFunctionMapping) -> Options:
    FuncHolder = create_func_holder(custom_functions)

    class CustomFunctions(
        functions.Functions,
        FuncHolder,  # type: ignore[misc,valid-type]
    ):
        pass

    return Options(custom_functions=CustomFunctions())

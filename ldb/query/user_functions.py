import os
import warnings
from pathlib import Path
from types import ModuleType
from typing import Iterable, Iterator, Set, Tuple

from ldb.exceptions import LDBException
from ldb.import_utils import import_path
from ldb.path import InstanceDir
from ldb.typing import JSONFuncMutableMapping

CUSTOM_FUNCTIONS_VAR = "CUSTOM_FUNCTIONS"


def load_user_functions(
    ldb_dir: Path,
    invalid_names: Iterable[str] = (),
) -> JSONFuncMutableMapping:
    invalid_names = set(invalid_names)
    user_functions: JSONFuncMutableMapping = {}
    try:
        for module, custom_functions in list(
            iter_user_function_modules(ldb_dir),
        ):
            validate_func_names(module, custom_functions, invalid_names)
            user_functions.update(custom_functions)
    except Exception as exc:
        raise LDBException(
            "Unable to load custom user functions: "
            f"{type(exc).__name__}:\n{exc}",
        ) from exc
    return user_functions


def iter_user_function_modules(
    ldb_dir: Path,
) -> Iterator[Tuple[ModuleType, JSONFuncMutableMapping]]:
    for path in sorted((ldb_dir / InstanceDir.USER_FUNCTIONS).glob("*.py")):
        module = import_path(os.fspath(path))
        try:
            custom_functions = getattr(module, CUSTOM_FUNCTIONS_VAR)
        except AttributeError:
            warnings.warn(
                f"Found user function module {module.__name__} "
                f"but no {CUSTOM_FUNCTIONS_VAR} variable: ",
            )
        else:
            yield module, custom_functions


def validate_func_names(
    module: ModuleType,
    func_names: Iterable[str],
    invalid_names: Set[str],
) -> None:
    for func_name in func_names:
        if func_name in invalid_names:
            raise ValueError(
                f"Invalid user function name: {func_name!r}\n"
                "A function with this name already exists.\n"
                f"(module_name={module.__name__!r}, "
                f"file_path={module.__file__!r})",
            )

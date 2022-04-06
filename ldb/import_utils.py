import importlib
import os
from types import ModuleType


def import_path(path: str, name: str = "") -> ModuleType:
    """
    Import a python file as a single-use module.

    This is intended for single-use imports and does not add the module to
    `sys.modules`. For files that may be imported more than once, make sure
    they're on the python path (`sys.path`) and use a regular import statement.

    See "Importing a source file directly" in the `importlib` documentation:
    https://docs.python.org/3.7/library/importlib.html#importing-a-source-file-directly
    """
    if not name:
        name = os.path.splitext(os.path.basename(path))[0]
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError("Could not load module with args: {name=}, {path=}")
    module = importlib.util.module_from_spec(spec)
    loader: importlib.abc.Loader = spec.loader
    loader.exec_module(module)
    return module

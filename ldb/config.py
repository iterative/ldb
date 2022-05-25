import os
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Dict, Generator, List, Optional, Sequence

from appdirs import site_config_dir, user_config_dir
from tomlkit import document, dumps, parse
from tomlkit.exceptions import NonExistentKey
from tomlkit.toml_document import TOMLDocument

from ldb.app_info import APP_AUTHOR, APP_NAME
from ldb.env import Env
from ldb.exceptions import LDBException, LDBInstanceNotFoundError
from ldb.path import DirName, Filename, GlobalDir


class ConfigType:
    INSTANCE = "INSTANCE"
    USER = "USER"
    SYSTEM = "SYSTEM"


DEFAULT_CONFIG_TYPES = (
    ConfigType.INSTANCE,
    ConfigType.USER,
    ConfigType.SYSTEM,
)
GLOBAL_CONFIG_TYPES = (
    ConfigType.USER,
    ConfigType.SYSTEM,
)


def load_from_path(path: Path) -> TOMLDocument:
    with path.open() as file:
        config_str = file.read()
    return parse(config_str)


def save_to_path(config: TOMLDocument, path: Path) -> None:
    config_str = dumps(config)
    if not path.parent.is_dir():
        path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as file:
        file.write(config_str)


def load_first(
    config_types: Sequence[str] = DEFAULT_CONFIG_TYPES,
    ldb_dir: Optional[Path] = None,
) -> Optional[TOMLDocument]:
    if ldb_dir is not None:
        path = ldb_dir / Filename.CONFIG
        try:
            return load_from_path(path)
        except FileNotFoundError:
            pass
    for config_type in config_types:
        for config_dir in get_config_dirs(config_type):
            path = config_dir / Filename.CONFIG
            try:
                return load_from_path(path)
            except FileNotFoundError:
                pass
    return None


@contextmanager
def edit(path: Path) -> Generator[TOMLDocument, None, None]:
    try:
        config = load_from_path(path)
    except FileNotFoundError:
        config = document()
    yield config
    save_to_path(config, path)


def _get_global_base_parent() -> Path:
    return Path.home()


def get_global_base() -> Path:
    return _get_global_base_parent() / DirName.GLOBAL_BASE


def get_default_instance_dir() -> Path:
    return get_global_base() / GlobalDir.DEFAULT_INSTANCE


def get_ldb_dir() -> Path:
    """
    Get the directory we should use as the ldb instance.
    """
    if Env.LDB_DIR in os.environ:
        return Path(os.environ[Env.LDB_DIR])
    config = load_first(GLOBAL_CONFIG_TYPES)
    if config is not None:
        try:
            ldb_dir_str: str = config["core"]["ldb_dir"]  # type: ignore[index,assignment] # noqa: E501
        except NonExistentKey:
            pass
        else:
            ldb_dir_path = Path(ldb_dir_str).expanduser()
            if ldb_dir_path.is_absolute():
                return ldb_dir_path
            raise LDBException(
                "Found relative path for core.ldb_dir: {repr(ldb_dir_str)}"
                "Paths in LDB config must be absolute",
            )
    return get_default_instance_dir()


def get_instance_config_dirs() -> List[Path]:
    from ldb.core import (  # pylint: disable=import-outside-toplevel
        get_ldb_instance,
    )

    try:
        return [get_ldb_instance()]
    except LDBInstanceNotFoundError:
        return []


def get_user_config_dirs() -> List[Path]:
    config_dirs = [get_global_base()]
    try:
        additional_dir = user_config_dir(APP_NAME, APP_AUTHOR)
    except OSError:
        pass
    else:
        config_dirs.append(Path(additional_dir))
    return config_dirs


def get_system_config_dirs() -> List[Path]:
    config_dirs = []
    try:
        additional_dir = site_config_dir(APP_NAME, APP_AUTHOR)
    except OSError:
        pass
    else:
        config_dirs.append(Path(additional_dir))
    return config_dirs


CONFIG_DIR_FUNCTIONS: Dict[str, Callable[[], List[Path]]] = {
    ConfigType.INSTANCE: get_instance_config_dirs,
    ConfigType.USER: get_user_config_dirs,
    ConfigType.SYSTEM: get_system_config_dirs,
}


def get_config_dirs(config_type: str) -> List[Path]:
    return CONFIG_DIR_FUNCTIONS[config_type]()


def set_default_instance(path: Path, overwrite_existing: bool = False) -> None:
    path = path.absolute()
    config_dir = get_global_base()
    config_dir.mkdir(parents=True, exist_ok=True)
    config_file = config_dir / Filename.CONFIG
    with edit(config_file) as cfg:
        if "core" in cfg:
            if (
                not overwrite_existing
                and "ldb_dir" in cfg["core"]  # type: ignore[operator]
            ):
                value: str = cfg["core"][  # type: ignore[index,assignment]
                    "ldb_dir"
                ]
                print(
                    "Not setting core.ldb_dir as it is already set "
                    f"to {repr(value)}",
                )
                return
        else:
            cfg["core"] = {}
        new_value = os.fspath(path)
        cfg["core"]["ldb_dir"] = new_value  # type: ignore[index]
    print(f"Set core.ldb_dir to {repr(new_value)}")

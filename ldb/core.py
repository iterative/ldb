import os
from pathlib import Path

from tomlkit.exceptions import NonExistentKey

from ldb.config import load_first_path
from ldb.env import Env
from ldb.path import INIT_CONFIG_TYPES, INSTANCE_DIR_NAME


class LabelDatabase:
    pass


def init(_path):
    pass


def find_init_location() -> Path:
    if Env.LDB_DIR in os.environ:
        return Path(os.environ[Env.LDB_DIR])

    config = load_first_path(INIT_CONFIG_TYPES)
    if config is not None:
        try:
            instance_dir = config["core"]["ldb_dir"]
        except NonExistentKey:
            pass
        else:
            return Path(instance_dir)
    return Path.home() / INSTANCE_DIR_NAME

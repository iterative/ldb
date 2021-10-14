import os
from pathlib import Path

from ldb.env import Env

LDB_DIR_NAME = ".ldb"


class Repo:
    pass


def init(_path):
    pass


def find_init_location() -> Path:
    loc = os.getenv(Env.LDB_DIR)
    if loc is not None:
        return Path(loc)

    loc = ""  # get from config
    if loc is not None:
        return Path(loc)

    return Path.home() / LDB_DIR_NAME

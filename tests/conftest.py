import os
from pathlib import Path
from typing import Generator, NoReturn

import pytest
from pytest import MonkeyPatch, TempPathFactory

from ldb.config import get_global_base, set_default_instance
from ldb.core import init
from ldb.env import Env
from ldb.main import main
from ldb.path import Filename
from ldb.storage import add_storage, create_storage_location
from ldb.utils import DATASET_PREFIX

from .utils import DATA_DIR, stage_new_workspace


def pytest_addoption(parser):
    parser.addoption(
        "--pyannotate",
        action="store_true",
        default=False,
        help="Collect typing info and output to type_info.json",
    )


def pytest_configure(config):
    if config.getoption("--pyannotate"):
        from . import (  # pylint: disable=import-outside-toplevel
            pyannotate_conftest,
        )

        config.pluginmanager.register(pyannotate_conftest)


@pytest.fixture(scope="session")
def monkeypatch_session() -> Generator[MonkeyPatch, None, None]:
    """Like monkeypatch, but for session scope."""
    mpatch = pytest.MonkeyPatch()
    yield mpatch
    mpatch.undo()


@pytest.fixture(scope="session", autouse=True)
def clean_environment(
    monkeypatch_session: MonkeyPatch,
    tmp_path_factory: TempPathFactory,
) -> None:
    """Make sure we have a clean environment and won't write to userspace."""
    working_dir = tmp_path_factory.mktemp("default_working_dir")
    os.chdir(working_dir)
    monkeypatch_session.delenv(Env.LDB_DIR, raising=False)

    def raise_exc() -> NoReturn:
        raise NotImplementedError(
            "To get a tmp path, use fixture global_base",
        )

    monkeypatch_session.setattr(
        "ldb.config._get_global_base_parent",
        raise_exc,
    )


@pytest.fixture
def global_base(monkeypatch: MonkeyPatch, tmp_path: Path) -> Path:
    def get_tmp_global_base_parent() -> Path:
        home = Path.home()
        return tmp_path / home.relative_to(home.anchor)

    monkeypatch.setattr(
        "ldb.config._get_global_base_parent",
        get_tmp_global_base_parent,
    )
    return get_global_base()


@pytest.fixture
def ldb_instance(tmp_path: Path, global_base: Path, data_dir: Path) -> Path:
    instance_dir = tmp_path / "ldb_instance"
    init(instance_dir)
    set_default_instance(instance_dir, overwrite_existing=True)
    storage_location = create_storage_location(path=os.fspath(data_dir))
    add_storage(instance_dir / Filename.STORAGE, storage_location)
    return instance_dir


@pytest.fixture
def data_dir() -> Path:
    return DATA_DIR


@pytest.fixture
def workspace_path(tmp_path: Path, ldb_instance: Path) -> Path:
    path = tmp_path / "workspace"
    stage_new_workspace(path)
    os.chdir(path)
    return path


@pytest.fixture
def index_original(ldb_instance: Path, data_dir: Path) -> Path:
    dir_to_index = data_dir / "fashion-mnist/original"
    main(["index", os.fspath(dir_to_index)])
    return dir_to_index


@pytest.fixture
def ds_a(workspace_path: Path, index_original: Path) -> str:
    ds_identifier = f"{DATASET_PREFIX}a"
    main(["stage", ds_identifier])
    main(
        [
            "add",
            "0x3c679fd1b8537dc7da1272a085e388e6",
            "0x982814b9116dce7882dfc31636c3ff7a",
            "0xebbc6c0cebb66738942ee56513f9ee2f",
            "0x1e0759182b328fd22fcdb5e6beb54adf",
        ],
    )
    main(["commit"])
    return ds_identifier


@pytest.fixture
def ds_b(workspace_path: Path, index_original: Path) -> str:
    ds_identifier = f"{DATASET_PREFIX}b"
    main(["stage", ds_identifier])
    main(
        [
            "add",
            "0x982814b9116dce7882dfc31636c3ff7a",
            "0x1e0759182b328fd22fcdb5e6beb54adf",
            "0x2f3533f1e35349602fbfaf0ec9b3ef3f",
            "0x95789bb1ac140460cefc97a6e66a9ee8",
            "0xe1c3ef93e4e1cf108fa2a4c9d6e03af2",
        ],
    )
    main(["commit"])
    return ds_identifier


@pytest.fixture
def staged_ds_fashion(workspace_path: Path) -> str:
    ds_identifier = f"{DATASET_PREFIX}fashion"
    main(["stage", ds_identifier])
    main(["add", os.fspath(DATA_DIR / "fashion-mnist")])
    return ds_identifier

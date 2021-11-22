import os
from pathlib import Path
from typing import Generator, NoReturn

import pytest
from pytest import MonkeyPatch, TempPathFactory

from ldb.config import get_global_base, set_default_instance
from ldb.core import init
from ldb.env import Env
from ldb.path import Filename
from ldb.stage import stage_workspace
from ldb.storage import add_storage, create_storage_location
from ldb.utils import current_time
from ldb.workspace import WorkspaceDataset


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
    return Path(__file__).parent.parent / "data"


@pytest.fixture
def workspace_path(tmp_path: Path, ldb_instance: Path) -> Path:
    path = tmp_path / "workspace"
    stage_workspace(
        path,
        WorkspaceDataset(
            dataset_name="my-dataset",
            staged_time=current_time(),
            parent="",
            tags=[],
        ),
    )
    os.chdir(path)
    return path

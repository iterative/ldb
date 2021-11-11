import os
from pathlib import Path

import pytest

from ldb.config import get_global_base, set_default_instance
from ldb.core import init
from ldb.env import Env
from ldb.stage import stage_workspace
from ldb.utils import current_time
from ldb.workspace import WorkspaceDataset


@pytest.fixture(scope="session")
def monkeypatch_session():
    """Like monkeypatch, but for session scope."""
    mpatch = pytest.MonkeyPatch()
    yield mpatch
    mpatch.undo()


@pytest.fixture(scope="session", autouse=True)
def clean_environment(monkeypatch_session, tmp_path_factory):
    """Make sure we have a clean environment and won't write to userspace."""
    working_dir = tmp_path_factory.mktemp("default_working_dir")
    os.chdir(working_dir)
    monkeypatch_session.delenv(Env.LDB_DIR, raising=False)

    def raise_exc():
        raise NotImplementedError(
            "To get a tmp path, use fixture global_base",
        )

    monkeypatch_session.setattr(
        "ldb.config._get_global_base_parent",
        raise_exc,
    )


@pytest.fixture
def global_base(monkeypatch, tmp_path):
    def get_tmp_global_base_parent():
        home = Path.home()
        return tmp_path / home.relative_to(home.anchor)

    monkeypatch.setattr(
        "ldb.config._get_global_base_parent",
        get_tmp_global_base_parent,
    )
    return get_global_base()


@pytest.fixture
def ldb_instance(tmp_path, global_base) -> Path:
    instance_dir = tmp_path / "ldb_instance"
    init(instance_dir)
    set_default_instance(instance_dir, overwrite_existing=True)
    return instance_dir


@pytest.fixture
def data_dir() -> Path:
    return Path(__file__).parent.parent / "data"


@pytest.fixture
def workspace_path(tmp_path, ldb_instance) -> Path:
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

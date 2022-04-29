import json
import os
from pathlib import Path
from typing import Generator, NoReturn

import pytest
from pytest import MonkeyPatch, TempPathFactory

from ldb.config import get_global_base, set_default_instance
from ldb.core import get_ldb_instance, init
from ldb.env import Env
from ldb.main import main
from ldb.path import Filename
from ldb.storage import add_storage, create_storage_location
from ldb.utils import DATASET_PREFIX, ROOT

from .utils import (
    DATA_DIR,
    add_user_filter,
    index_fashion_mnist,
    stage_new_workspace,
)


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
    """
    Like monkeypatch, but for session scope.
    """
    mpatch = pytest.MonkeyPatch()
    yield mpatch
    mpatch.undo()


@pytest.fixture(scope="session")
def tmp_path_session(tmp_path_factory: TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("tmp_path_session", numbered=False)


@pytest.fixture(scope="session", autouse=True)
def clean_environment(
    monkeypatch_session: MonkeyPatch,
    tmp_path_factory: TempPathFactory,
) -> None:
    """
    Make sure we have a clean environment and won't write to userspace.
    """
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


def make_global_base(mp: MonkeyPatch, path: Path) -> Path:
    def get_tmp_global_base_parent() -> Path:
        home = Path.home()
        return path / home.relative_to(home.anchor)

    mp.setattr(
        "ldb.config._get_global_base_parent",
        get_tmp_global_base_parent,
    )
    return get_global_base()


@pytest.fixture(scope="session")
def global_base_session(
    monkeypatch_session: MonkeyPatch,
    tmp_path_session: Path,
) -> Path:
    return make_global_base(monkeypatch_session, tmp_path_session)


@pytest.fixture
def global_base(monkeypatch: MonkeyPatch, tmp_path: Path) -> Path:
    return make_global_base(monkeypatch, tmp_path)


def make_ldb_instance(path: Path) -> Path:
    instance_dir = path / "ldb_instance"
    init(instance_dir, auto_index=True)
    set_default_instance(instance_dir, overwrite_existing=True)
    storage_location = create_storage_location(path=os.fspath(DATA_DIR))
    add_storage(instance_dir / Filename.STORAGE, storage_location)
    return instance_dir


@pytest.fixture
def ldb_instance(tmp_path: Path, global_base: Path) -> Path:
    return make_ldb_instance(tmp_path)


@pytest.fixture(scope="session")
def ldb_instance_session(
    tmp_path_session: Path,
    global_base_session: Path,
) -> Path:
    ldb_dir = make_ldb_instance(tmp_path_session)
    add_user_filter(ldb_dir)
    return ldb_dir


@pytest.fixture
def data_dir() -> Path:
    return DATA_DIR


def make_workspace_path(path: Path, name: str = "workspace") -> Path:
    path = path / name
    stage_new_workspace(path)
    os.chdir(path)
    return path


@pytest.fixture
def workspace_path(tmp_path: Path, ldb_instance: Path) -> Path:
    return make_workspace_path(tmp_path)


@pytest.fixture
def global_workspace_path(tmp_path: Path, ldb_instance_session: Path) -> Path:
    return make_workspace_path(tmp_path)


@pytest.fixture
def index_original(ldb_instance: Path, data_dir: Path) -> Path:
    dir_to_index = data_dir / "fashion-mnist/original"
    main(["index", "-m", "bare", os.fspath(dir_to_index)])
    return dir_to_index


@pytest.fixture
def ds_a(workspace_path: Path, index_original: Path) -> str:
    ds_identifier = f"{DATASET_PREFIX}a"
    main(["stage", ds_identifier])
    main(
        [
            "add",
            "id:3c679fd1b8537dc7da1272a085e388e6",
            "id:982814b9116dce7882dfc31636c3ff7a",
            "id:ebbc6c0cebb66738942ee56513f9ee2f",
            "id:1e0759182b328fd22fcdb5e6beb54adf",
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
            "id:982814b9116dce7882dfc31636c3ff7a",
            "id:1e0759182b328fd22fcdb5e6beb54adf",
            "id:2f3533f1e35349602fbfaf0ec9b3ef3f",
            "id:95789bb1ac140460cefc97a6e66a9ee8",
            "id:e1c3ef93e4e1cf108fa2a4c9d6e03af2",
        ],
    )
    main(["commit"])
    return ds_identifier


@pytest.fixture
def staged_ds_fashion(workspace_path: Path) -> str:
    ds_identifier = f"{DATASET_PREFIX}fashion"
    main(["stage", ds_identifier])
    index_fashion_mnist(get_ldb_instance())
    main(["add", f"{DATASET_PREFIX}{ROOT}"])
    return ds_identifier


@pytest.fixture
def label_studio_json_path(tmp_path: Path) -> Path:
    parent = DATA_DIR / "flat-data-object-only"
    with open(
        DATA_DIR / "label-studio/json-format.json",
        encoding="utf-8",
    ) as f:
        annot_str = f.read()
    annot = json.loads(annot_str)
    for task in annot:
        task["data"]["image"] = os.fspath(
            parent / task["data"]["image"].rsplit("/", 1)[1],
        )

    json_out = json.dumps(annot, indent=2)
    dest = tmp_path / "data/json-format.json"
    dest.parent.mkdir(parents=True)
    with open(dest, "x", encoding="utf-8") as f:
        f.write(json_out)
    return dest


@pytest.fixture(scope="session")
def fashion_mnist_session(ldb_instance_session: Path) -> Path:
    index_fashion_mnist(ldb_instance_session)
    return ldb_instance_session

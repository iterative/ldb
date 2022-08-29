import os
import sys
from pathlib import Path
from typing import List

import pytest
from pytest import TempPathFactory

from ldb.main import main
from ldb.utils import DATASET_PREFIX, ROOT, chdir
from tests.conftest import make_ldb_instance
from tests.utils import BENCH_DATA_DIR, add_user_filter, stage_new_workspace


def run(argv: List[str]) -> int:
    ret = main([*argv, "-vv"])
    if ret != 0:
        sys.exit(ret)
    return ret


@pytest.fixture(scope="session")
def ldb_instance_bench_session(
    tmp_path_factory: TempPathFactory,
    global_base_session: Path,
) -> Path:
    path = tmp_path_factory.mktemp("tmp_path_bench_session", numbered=False)
    ldb_dir = make_ldb_instance(path / "index")
    add_user_filter(ldb_dir)
    return ldb_dir


@pytest.fixture(scope="session")
def numbers_ds_session(ldb_instance_session: Path) -> Path:
    run(["index", "-m", "bare", os.fspath(BENCH_DATA_DIR / "numbers-10000")])
    ws_path = ldb_instance_session.parent / "workspace"
    stage_new_workspace(ws_path, "numbers")
    with chdir(ws_path):
        run(["add", f"{DATASET_PREFIX}{ROOT}"])
        run(["commit"])
    return ldb_instance_session

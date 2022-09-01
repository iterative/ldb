import pytest

from ldb.main import main
from ldb.utils import DATASET_PREFIX, ROOT

BENCH_DATA = {
    "no-args": [],
    "query1": ["--query", "number <= `5000`"],
    "path1": ["--path", r"3\.txt$"],
}


def bench(bench_func, func):
    return bench_func(func, warmup_rounds=1, rounds=2, iterations=3)


@pytest.mark.parametrize(
    "args",
    BENCH_DATA.values(),
    ids=BENCH_DATA.keys(),
)
def test_cli_ls_root_dataset(
    args,
    numbers_ds_session,
    benchmark,
):
    base_args = ["list", f"{DATASET_PREFIX}{ROOT}", "-s"]

    def func():
        return main([*base_args, *args])

    ret = bench(benchmark.pedantic, func)
    assert ret == 0


@pytest.mark.parametrize(
    "args",
    BENCH_DATA.values(),
    ids=BENCH_DATA.keys(),
)
def test_cli_ls_dataset(
    args,
    numbers_ds_session,
    benchmark,
):
    base_args = ["list", f"{DATASET_PREFIX}numbers", "-s"]

    def func():
        return main([*base_args, *args])

    ret = bench(benchmark.pedantic, func)
    assert ret == 0

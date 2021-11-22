"""
pytest configuration for collecting typing info with pyannotate.

See:
https://github.com/dropbox/pyannotate/blob/master/example/example_conftest.py
"""
# pylint: disable=import-outside-toplevel
import pytest


def pytest_collection_finish(session):  # pylint: disable=unused-argument
    """
    Handle the pytest collection finish hook: configure pyannotate.

    Explicitly delay importing `collect_types` until all tests have been
    collected.  This gives gevent a chance to monkey patch the world
    before importing pyannotate.
    """
    from pyannotate_runtime import collect_types

    collect_types.init_types_collection()


@pytest.fixture(autouse=True)
def collect_types_fixture():
    from pyannotate_runtime import collect_types

    collect_types.start()
    yield
    collect_types.stop()


def pytest_sessionfinish(
    session,
    exitstatus,
):  # pylint: disable=unused-argument
    from pyannotate_runtime import collect_types

    collect_types.dump_stats("type_info.json")

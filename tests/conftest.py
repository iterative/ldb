from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def monkeypatch_session():
    """Like monkeypatch, but for session scope."""
    mpatch = pytest.MonkeyPatch()
    yield mpatch
    mpatch.undo()


@pytest.fixture(scope="session", autouse=True)
def error_on_get_global_base_parent(monkeypatch_session):
    """Make sure we don't use actual userspace for file creation."""

    def raise_exc():
        raise NotImplementedError(
            "To get a tmp path, use fixture mock_get_global_base_parent",
        )

    monkeypatch_session.setattr("ldb.config.get_global_base_parent", raise_exc)


@pytest.fixture
def mock_get_global_base_parent(monkeypatch, tmp_path):
    def get_tmp_global_base_parent():
        home = Path.home()
        return tmp_path / home.relative_to(home.anchor)

    monkeypatch.setattr(
        "ldb.config.get_global_base_parent",
        get_tmp_global_base_parent,
    )
    return get_tmp_global_base_parent

import os

from ldb import config
from ldb.core import is_ldb_instance
from ldb.env import Env
from ldb.main import main
from ldb.path import Filename, GlobalDir


def test_init_command_default(mock_get_global_base_parent):
    ret = main(["init"])
    instance_dir = mock_get_global_base_parent() / GlobalDir.DEFAULT_INSTANCE
    assert ret == 0
    assert is_ldb_instance(instance_dir)


def test_init_command_with_ldb_dir_env_var(monkeypatch, tmp_path):
    instance_dir = tmp_path / "alternate" / "location"
    monkeypatch.setenv(Env.LDB_DIR, os.fspath(instance_dir))
    ret = main(["init"])
    assert ret == 0
    assert is_ldb_instance(instance_dir)


def test_init_command_with_ldb_dir_config_var(
    mock_get_global_base_parent,
    tmp_path,
):
    instance_dir = tmp_path / "different" / "location"
    config_path = (
        mock_get_global_base_parent() / GlobalDir.BASE / Filename.CONFIG
    )
    with config.edit(config_path) as cfg:
        cfg["core"] = {"ldb_dir": os.fspath(instance_dir)}
    ret = main(["init"])
    assert ret == 0
    assert is_ldb_instance(instance_dir)


def test_init_command_with_path(tmp_path):
    instance_dir = tmp_path / "some" / "location"
    ret = main(["init", os.fspath(instance_dir)])
    assert ret == 0
    assert is_ldb_instance(instance_dir)


def test_init_command_default_existing_instance(mock_get_global_base_parent):
    main(["init"])
    ret = main(["init"])
    assert ret == 1


def test_init_command_force_existing_instance(mock_get_global_base_parent):
    main(["init"])
    instance_dir = mock_get_global_base_parent() / GlobalDir.DEFAULT_INSTANCE
    new_filepath = instance_dir / "file"
    new_filepath.touch()
    ret = main(["init", "--force"])
    assert ret == 0
    assert is_ldb_instance(instance_dir)
    assert not new_filepath.exists()

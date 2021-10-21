import os

from ldb.core import is_ldb_instance
from ldb.main import main


def test_init_command_new_location(tmp_path):
    instance_dir = tmp_path / "some" / "location"
    ret = main(["init", os.fspath(instance_dir)])
    assert ret == 0
    assert is_ldb_instance(instance_dir)


def test_init_command_rel_path(tmp_path):
    instance_dir = tmp_path / "some" / "location"
    instance_dir.mkdir(parents=True)
    os.chdir(instance_dir)
    ret = main(["init", "."])
    assert ret == 0
    assert is_ldb_instance(instance_dir)


def test_init_command_existing_instance(tmp_path):
    instance_dir = tmp_path / "some" / "location"
    argv = ["init", os.fspath(instance_dir)]
    ret1 = main(argv)
    ret2 = main(argv)
    assert ret1 == 0
    assert ret2 == 1


def test_init_command_force_existing_instance(tmp_path):
    instance_dir = tmp_path / "some" / "location"
    ret1 = main(["init", os.fspath(instance_dir)])
    new_filepath = instance_dir / "file"
    new_filepath.touch()
    ret2 = main(["init", os.fspath(instance_dir), "--force"])
    assert ret1 == 0
    assert ret2 == 0
    assert is_ldb_instance(instance_dir)
    assert not new_filepath.exists()

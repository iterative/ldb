import os

from tomlkit import document
from tomlkit.toml_document import TOMLDocument

from ldb import config
from ldb.config import ConfigType
from ldb.core import is_ldb_instance
from ldb.main import main
from ldb.path import Filename


def test_init_command_new_location(tmp_path, global_base):
    instance_dir = tmp_path / "some" / "location"
    ret = main(["init", os.fspath(instance_dir)])
    global_cfg = config.load_from_path(
        config.get_global_base() / Filename.CONFIG,
    )
    ldb_dir = global_cfg["core"]["ldb_dir"]  # type: ignore[index]
    cfg: TOMLDocument = config.load_first([ConfigType.INSTANCE]) or document()
    assert ret == 0
    assert is_ldb_instance(instance_dir)
    assert ldb_dir == os.fspath(instance_dir)
    assert not cfg["core"]["read_any_cloud_location"]  # type: ignore[index]
    assert not cfg["core"]["auto_index"]  # type: ignore[index]


def test_init_command_rel_path(tmp_path, global_base):
    instance_dir = tmp_path / "some" / "location"
    instance_dir.mkdir(parents=True)
    os.chdir(instance_dir)
    ret = main(["init", "."])
    assert ret == 0
    assert is_ldb_instance(instance_dir)


def test_init_command_existing_instance(tmp_path, global_base):
    instance_dir = tmp_path / "some" / "location"
    argv = ["init", os.fspath(instance_dir)]
    ret1 = main(argv)
    ret2 = main(argv)
    assert ret1 == 0
    assert ret2 == 1


def test_init_command_force_existing_instance(
    tmp_path,
    global_base,
):
    instance_dir = tmp_path / "some" / "location"
    ret1 = main(["init", os.fspath(instance_dir)])
    new_filepath = instance_dir / "file"
    new_filepath.touch()
    ret2 = main(["init", os.fspath(instance_dir), "--force"])
    assert ret1 == 0
    assert ret2 == 0
    assert is_ldb_instance(instance_dir)
    assert not new_filepath.exists()

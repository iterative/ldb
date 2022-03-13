import os
from dataclasses import asdict

from fsspec.implementations.local import make_path_posix

from ldb import storage
from ldb.main import main
from ldb.path import Filename, GlobalDir
from ldb.utils import chdir


def test_add_storage_command_default(global_base, tmp_path):
    path1 = tmp_path / "a" / "b" / "c"
    path2 = tmp_path / "a" / "b" / "d"
    path1.mkdir(parents=True, exist_ok=True)
    path2.mkdir(parents=True, exist_ok=True)
    ret1 = main(["add-storage", os.fspath(path1)])
    ret2 = main(["add-storage", os.fspath(path2)])
    storage_config = storage.load_from_path(
        global_base / GlobalDir.DEFAULT_INSTANCE / Filename.STORAGE,
    )
    config_dict = asdict(storage_config)
    expected = {
        "locations": [
            {
                "path": make_path_posix(os.fspath(path1)),
                "protocol": "file",
                "fs_id": "",
                "read_access_verified": True,
                "write_access_verified": True,
                "read_and_add": False,
                "options": {},
            },
            {
                "path": make_path_posix(os.fspath(path2)),
                "protocol": "file",
                "fs_id": "",
                "read_access_verified": True,
                "write_access_verified": True,
                "read_and_add": False,
                "options": {},
            },
        ],
    }
    assert ret1 == 0
    assert ret2 == 0
    assert config_dict == expected


def test_add_storage_command_unnormalized(global_base, tmp_path):
    working_dir = tmp_path / "a"
    path = working_dir / "e"
    path.mkdir(parents=True, exist_ok=True)
    with chdir(working_dir):
        ret = main(["add-storage", "../a/b/c/../../d/../e"])
    storage_config = storage.load_from_path(
        global_base / GlobalDir.DEFAULT_INSTANCE / Filename.STORAGE,
    )
    config_dict = asdict(storage_config)
    expected = {
        "locations": [
            {
                "path": make_path_posix(os.fspath(path)),
                "protocol": "file",
                "fs_id": "",
                "read_access_verified": True,
                "write_access_verified": True,
                "read_and_add": False,
                "options": {},
            },
        ],
    }
    assert ret == 0
    assert config_dict == expected


def test_add_storage_command_add_update(global_base, tmp_path):
    path = tmp_path / "a" / "b" / "c"
    path.mkdir(parents=True, exist_ok=True)
    ret1 = main(["add-storage", os.fspath(path)])
    ret2 = main(["add-storage", "--read-add", os.fspath(path)])
    storage_config = storage.load_from_path(
        global_base / GlobalDir.DEFAULT_INSTANCE / Filename.STORAGE,
    )
    config_dict = asdict(storage_config)
    expected = {
        "locations": [
            {
                "path": make_path_posix(os.fspath(path)),
                "protocol": "file",
                "fs_id": "",
                "read_access_verified": True,
                "write_access_verified": True,
                "read_and_add": True,
                "options": {},
            },
        ],
    }
    assert ret1 == 0
    assert ret2 == 0
    assert config_dict == expected


def test_add_storage_command_child(global_base, tmp_path):
    path1 = tmp_path / "a"
    path2 = tmp_path / "a" / "b" / "c"
    path1.mkdir(parents=True, exist_ok=True)
    path2.mkdir(parents=True, exist_ok=True)
    ret1 = main(["add-storage", os.fspath(path1)])
    ret2 = main(["add-storage", os.fspath(path2)])
    storage_config = storage.load_from_path(
        global_base / GlobalDir.DEFAULT_INSTANCE / Filename.STORAGE,
    )
    config_dict = asdict(storage_config)
    expected = {
        "locations": [
            {
                "path": make_path_posix(os.fspath(path1)),
                "protocol": "file",
                "fs_id": "",
                "read_access_verified": True,
                "write_access_verified": True,
                "read_and_add": False,
                "options": {},
            },
        ],
    }
    assert ret1 == 0
    assert ret2 == 1
    assert config_dict == expected


def test_add_storage_command_parent(global_base, tmp_path):
    path1 = tmp_path / "a" / "b" / "c"
    path2 = tmp_path / "a"
    path1.mkdir(parents=True, exist_ok=True)
    path2.mkdir(parents=True, exist_ok=True)
    ret1 = main(["add-storage", os.fspath(path1)])
    ret2 = main(["add-storage", os.fspath(path2)])
    storage_config = storage.load_from_path(
        global_base / GlobalDir.DEFAULT_INSTANCE / Filename.STORAGE,
    )
    config_dict = asdict(storage_config)
    expected = {
        "locations": [
            {
                "path": make_path_posix(os.fspath(path1)),
                "protocol": "file",
                "fs_id": "",
                "read_access_verified": True,
                "write_access_verified": True,
                "read_and_add": False,
                "options": {},
            },
        ],
    }
    assert ret1 == 0
    assert ret2 == 1
    assert config_dict == expected


def test_add_storage_command_force_parent(
    global_base,
    tmp_path,
):
    path1 = tmp_path / "a" / "b" / "c"
    path2 = tmp_path / "a"
    path1.mkdir(parents=True, exist_ok=True)
    path2.mkdir(parents=True, exist_ok=True)
    ret1 = main(["add-storage", os.fspath(path1)])
    ret2 = main(["add-storage", "--force", os.fspath(path2)])
    storage_config = storage.load_from_path(
        global_base / GlobalDir.DEFAULT_INSTANCE / Filename.STORAGE,
    )
    config_dict = asdict(storage_config)
    expected = {
        "locations": [
            {
                "path": make_path_posix(os.fspath(path2)),
                "protocol": "file",
                "fs_id": "",
                "read_access_verified": True,
                "write_access_verified": True,
                "read_and_add": False,
                "options": {},
            },
        ],
    }
    assert ret1 == 0
    assert ret2 == 0
    assert config_dict == expected

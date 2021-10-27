import os
from pathlib import Path

from ldb.main import main
from ldb.path import InstanceDir
from ldb.utils import load_data_file

DATA_OBJECT_KEYS = (
    "type",
    "first_indexed",
    "last_indexed",
    "last_indexed_by",
    "tags",
    "alternate_paths",
    "fs",
)
DATA_OBJECT_FS_KEYS = (
    "fs_id",
    "protocol",
    "path",
    "size",
    "mode",
    "uid",
    "gid",
    "atime",
    "mtime",
    "ctime",
)
ANNOTATION_META_KEYS = (
    "version",
    "mtime",
    "first_indexed_time",
    "last_indexed_time",
)
ANNOTATION_LDB_KEYS = (
    "user_version",
    "schema_version",
)


def is_data_object_meta(file_path: Path) -> bool:
    data = load_data_file(file_path)
    return (
        tuple(data.keys()) == DATA_OBJECT_KEYS
        and tuple(data["fs"].keys()) == DATA_OBJECT_FS_KEYS
    )


def is_annotation_meta(file_path: Path) -> bool:
    return (
        tuple(load_data_file(file_path).keys()) == ANNOTATION_META_KEYS
        and (file_path.parent.parent / "current").is_file()
    )


def is_annotation(dir_path: Path):
    return (
        tuple(
            load_data_file(dir_path / "ldb"),
        )
        == ANNOTATION_LDB_KEYS
        and bool(load_data_file(dir_path / "user"))
    )


def get_data_object_meta_file_paths(ldb_instance):
    return list((ldb_instance / InstanceDir.DATA_OBJECT_INFO).glob("*/*/meta"))


def get_annotation_meta_file_paths(ldb_instance):
    return list(
        (ldb_instance / InstanceDir.DATA_OBJECT_INFO).glob(
            "*/*/annotations/*",
        ),
    )


def get_annotation_dir_paths(ldb_instance):
    return list((ldb_instance / InstanceDir.ANNOTATIONS).glob("*/*"))


def get_indexed_data_paths(ldb_dir):
    return (
        get_data_object_meta_file_paths(ldb_dir),
        get_annotation_meta_file_paths(ldb_dir),
        get_annotation_dir_paths(ldb_dir),
    )


def test_index_first_time(ldb_instance, data_dir):
    path = data_dir / "fashion-mnist" / "original"
    ret = main(["index", f"{os.fspath(path)}"])

    (
        data_object_meta_paths,
        annotation_meta_paths,
        annotation_paths,
    ) = get_indexed_data_paths(ldb_instance)
    non_data_object_meta = [
        p for p in data_object_meta_paths if not is_data_object_meta(p)
    ]
    non_annotation_meta = [
        p for p in annotation_meta_paths if not is_annotation_meta(p)
    ]
    non_annotation = [p for p in annotation_paths if not is_annotation(p)]

    assert ret == 0
    assert len(data_object_meta_paths) == 32
    assert len(annotation_meta_paths) == 23
    assert len(annotation_paths) == 10
    assert non_data_object_meta == []
    assert non_annotation_meta == []
    assert non_annotation == []


def test_index_twice(ldb_instance, data_dir):
    path1 = data_dir / "fashion-mnist" / "original"
    path2 = data_dir / "fashion-mnist" / "updates"
    ret1 = main(["index", f"{os.fspath(path1)}"])
    ret2 = main(["index", f"{os.fspath(path2)}"])

    (
        data_object_meta_paths,
        annotation_meta_paths,
        annotation_paths,
    ) = get_indexed_data_paths(ldb_instance)

    assert ret1 == 0
    assert ret2 == 0
    assert len(data_object_meta_paths) == 32
    assert len(annotation_meta_paths) == 27
    assert len(annotation_paths) == 14


def test_index_same_location_twice(ldb_instance, data_dir):
    path = data_dir / "fashion-mnist" / "original"
    ret1 = main(["index", f"{os.fspath(path)}"])
    paths1 = get_indexed_data_paths(ldb_instance)
    data_object_meta1 = load_data_file(paths1[0][0])

    ret2 = main(["index", f"{os.fspath(path)}"])
    paths2 = get_indexed_data_paths(ldb_instance)
    data_object_meta2 = load_data_file(paths1[0][0])
    assert ret1 == 0
    assert ret2 == 0
    assert paths1 == paths2
    assert (
        data_object_meta2["first_indexed"]
        == data_object_meta1["first_indexed"]
    )
    assert (
        data_object_meta2["last_indexed"] > data_object_meta1["last_indexed"]
    )

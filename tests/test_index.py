import os
import shutil

from ldb.main import main
from ldb.path import Filename
from ldb.storage import add_storage, create_storage_location
from ldb.utils import chdir, load_data_file

from .utils import (
    get_indexed_data_paths,
    is_annotation,
    is_annotation_meta,
    is_data_object_meta,
)


def test_index_first_time(ldb_instance, data_dir):
    path = os.fspath(data_dir / "fashion-mnist" / "original")
    ret = main(["index", "-m", "bare", path])
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
    path1 = os.fspath(data_dir / "fashion-mnist" / "original")
    path2 = os.fspath(data_dir / "fashion-mnist" / "updates")
    ret1 = main(["index", "-m", "bare", path1])
    ret2 = main(["index", "-m", "bare", path2])
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
    path = os.fspath(data_dir / "fashion-mnist" / "original")
    ret1 = main(["index", "-m", "bare", path])
    paths1 = get_indexed_data_paths(ldb_instance)
    data_object_meta1 = load_data_file(paths1[0][0])

    ret2 = main(["index", "-m", "bare", path])
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


def test_index_annotation_file(ldb_instance, data_dir):
    path = os.fspath(
        data_dir / "fashion-mnist/original/has_both/train/00002.json",
    )
    ret = main(["index", "-m", "bare", path])
    (
        data_object_meta_paths,
        annotation_meta_paths,
        annotation_paths,
    ) = get_indexed_data_paths(ldb_instance)

    assert ret == 0
    assert len(data_object_meta_paths) == 1
    assert len(annotation_meta_paths) == 1
    assert len(annotation_paths) == 1


def test_index_annotation_file_without_data_object(ldb_instance, data_dir):
    path = os.fspath(
        data_dir / "fashion-mnist/original/annotations_only/01011.json",
    )
    ret = main(["index", "-m", "bare", path])
    (
        data_object_meta_paths,
        annotation_meta_paths,
        annotation_paths,
    ) = get_indexed_data_paths(ldb_instance)

    assert ret == 0
    assert len(data_object_meta_paths) == 0
    assert len(annotation_meta_paths) == 0
    assert len(annotation_paths) == 0


def test_index_data_object_file(ldb_instance, data_dir):
    path = os.fspath(
        data_dir / "fashion-mnist/original/has_both/train/00002.png",
    )
    ret = main(["index", "-m", "bare", path])
    (
        data_object_meta_paths,
        annotation_meta_paths,
        annotation_paths,
    ) = get_indexed_data_paths(ldb_instance)

    assert ret == 0
    assert len(data_object_meta_paths) == 1
    assert len(annotation_meta_paths) == 1
    assert len(annotation_paths) == 1


def test_index_annotation_file_glob(ldb_instance, data_dir):
    path = os.fspath(data_dir / "fashion-mnist/original/*/t*/000[0-1]*.json")
    ret = main(["index", "-m", "bare", path])
    (
        data_object_meta_paths,
        annotation_meta_paths,
        annotation_paths,
    ) = get_indexed_data_paths(ldb_instance)

    assert ret == 0
    assert len(data_object_meta_paths) == 10
    assert len(annotation_meta_paths) == 10
    assert len(annotation_paths) == 6


def test_index_glob_dir_path(ldb_instance, data_dir):
    path = os.fspath(data_dir / "fashion-mnist/original/*/t*/")
    ret = main(["index", "-m", "bare", path])
    (
        data_object_meta_paths,
        annotation_meta_paths,
        annotation_paths,
    ) = get_indexed_data_paths(ldb_instance)

    assert ret == 0
    assert len(data_object_meta_paths) == 23
    assert len(annotation_meta_paths) == 23
    assert len(annotation_paths) == 10


def test_index_hidden_paths(ldb_instance, data_dir, tmp_path):
    src_path = data_dir / "fashion-mnist/original/has_both/train"
    storage_path = tmp_path / "storage"
    storage_location = create_storage_location(path=os.fspath(storage_path))
    add_storage(ldb_instance / Filename.STORAGE, storage_location)
    path_pairs = [
        ("00002", ".00002"),
        ("00007", "dir/.dir/00007"),
        ("00010", "dir/.dir/.00010"),
        ("00015", "dir/dir/.00015"),
        ("00016", "dir/dir/00016"),
    ]
    for src, dest in path_pairs:
        dest_path = storage_path / dest
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        for ext in ".json", ".png":
            shutil.copy2(
                (src_path / src).with_suffix(ext),
                dest_path.with_suffix(ext),
            )
    ret = main(["index", "-m", "bare", os.fspath(storage_path)])
    (
        data_object_meta_paths,
        annotation_meta_paths,
        annotation_paths,
    ) = get_indexed_data_paths(ldb_instance)
    assert ret == 0
    assert len(data_object_meta_paths) == 1
    assert len(annotation_meta_paths) == 1
    assert len(annotation_paths) == 1


def test_index_ephemeral_location(ldb_instance, data_dir, tmp_path):
    storage_path = os.fspath(tmp_path / "ephemeral_location")
    shutil.copytree(data_dir / "fashion-mnist/original/has_both", storage_path)

    read_add_path = tmp_path / "read-add-storage"
    add_storage(
        ldb_instance / Filename.STORAGE,
        create_storage_location(
            path=os.fspath(read_add_path),
            read_and_add=True,
        ),
    )

    ret = main(["index", "-m", "bare", storage_path])

    read_add_index_base = list(read_add_path.glob("ldb-autoimport/*/*"))[0]
    num_read_add_annotation_files = len(
        list(read_add_index_base.glob("**/*.json")),
    )
    num_read_add_data_object_files = len(
        list(read_add_index_base.glob("**/*.png")),
    )
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
    assert len(data_object_meta_paths) == 23
    assert len(annotation_meta_paths) == 23
    assert len(annotation_paths) == 10
    assert non_data_object_meta == []
    assert non_annotation_meta == []
    assert non_annotation == []
    assert num_read_add_data_object_files == 23
    assert num_read_add_annotation_files == 23


def test_index_relative_path(ldb_instance, data_dir):
    path = os.fspath(data_dir / "fashion-mnist" / "original")
    with chdir(path):
        ret = main(["index", "-m", "bare", "."])

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


def test_index_annotation_only(ldb_instance, data_dir):
    main(["index", "-m", "bare", os.fspath(data_dir / "data-object-only")])
    ret = main(
        ["index", "-m", "annot", os.fspath(data_dir / "annotation-only")],
    )
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
    assert len(annotation_meta_paths) == 29
    assert len(annotation_paths) == 16
    assert non_data_object_meta == []
    assert non_annotation_meta == []
    assert non_annotation == []

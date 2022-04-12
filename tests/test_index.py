import os
import shutil
from typing import NamedTuple

import pytest

from ldb.add import get_current_annotation_hashes
from ldb.data_formats import Format
from ldb.dataset import get_annotations
from ldb.index import index
from ldb.index.base import IndexingResult
from ldb.main import main
from ldb.path import Filename
from ldb.storage import add_storage, create_storage_location
from ldb.utils import chdir, load_data_file

from .utils import (
    DATA_DIR,
    get_indexed_data_paths,
    get_obj_tags,
    is_annotation,
    is_annotation_meta,
    is_data_object_meta,
)


class IndexingNums(NamedTuple):
    num_found_data_objects: int
    num_found_annotations: int
    num_new_data_objects: int
    num_new_annotations: int
    len_data_object_hashes: int

    @classmethod
    def from_result(cls, result: IndexingResult) -> "IndexingNums":
        return cls(
            result.num_found_data_objects,
            result.num_found_annotations,
            result.num_new_data_objects,
            result.num_new_annotations,
            len(result.data_object_hashes),
        )


@pytest.mark.parametrize(
    "params",
    [
        [(Format.AUTO, "fashion-mnist/original", (23, 23, 23, 23, 23))],
        [(Format.STRICT, "fashion-mnist/original", (23, 23, 23, 23, 23))],
        [(Format.BARE, "fashion-mnist/original", (32, 23, 32, 23, 32))],
        [(Format.INFER, "inferred/multilevel", (23, 23, 23, 23, 23))],
        [
            (Format.BARE, "data-object-only/original", (32, 0, 32, 0, 32)),
            (Format.ANNOT, "annotation-only/original", (0, 23, 0, 23, 23)),
        ],
        [
            (Format.BARE, "data-object-only/original", (32, 0, 32, 0, 32)),
            (Format.AUTO, "annotation-only/original", (0, 23, 0, 23, 23)),
        ],
    ],
)
def test_index_func_single_path(params, ldb_instance):
    results = []
    expected_results = []
    for fmt, path, expected in params:
        results.append(
            IndexingNums.from_result(
                index(ldb_instance, [str(DATA_DIR / path)], False, fmt),
            ),
        )
        expected_results.append(IndexingNums(*expected))
    assert results == expected_results


@pytest.mark.parametrize(
    "path,counts",
    [
        ("fashion-mnist/original", (23, 23, 10)),
        ("cases/same-obj-without-ext", (1, 2, 2)),
    ],
)
def test_cli_index_default(path, counts, ldb_instance, data_dir):
    ret = main(["index", os.fspath(data_dir / path)])
    real_counts = tuple(map(len, get_indexed_data_paths(ldb_instance)))
    assert ret == 0
    assert real_counts == counts


def test_index_func_single_path_label_studio(
    label_studio_json_path,
    ldb_instance,
):
    result = IndexingNums.from_result(
        index(
            ldb_instance,
            [str(DATA_DIR / label_studio_json_path)],
            False,
            Format.LABEL_STUDIO,
        ),
    )
    assert result == IndexingNums(23, 23, 23, 23, 23)


def test_index_bare(ldb_instance, data_dir):
    path = os.fspath(data_dir / "fashion-mnist" / "original")
    ret = main(["index", "-m", "bare", "--add-tag=img", path])
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
    tag_seqs = get_obj_tags(data_object_meta_paths)

    assert ret == 0
    assert len(data_object_meta_paths) == 32
    assert len(annotation_meta_paths) == 23
    assert len(annotation_paths) == 10
    assert non_data_object_meta == []
    assert non_annotation_meta == []
    assert non_annotation == []
    assert tag_seqs == [["img"]] * len(tag_seqs)


def test_index_twice(ldb_instance, data_dir):
    path1 = os.fspath(data_dir / "fashion-mnist" / "original")
    path2 = os.fspath(data_dir / "fashion-mnist" / "updates")
    ret1 = main(["index", "-m", "bare", "--add-tags=a,b", path1])
    ret2 = main(["index", "-m", "bare", "--add-tags=c", path2])
    (
        data_object_meta_paths,
        annotation_meta_paths,
        annotation_paths,
    ) = get_indexed_data_paths(ldb_instance)
    tag_seqs = get_obj_tags(data_object_meta_paths)
    unique_tag_seqs = set(map(tuple, tag_seqs))  # type: ignore[arg-type]

    assert ret1 == 0
    assert ret2 == 0
    assert len(data_object_meta_paths) == 32
    assert len(annotation_meta_paths) == 27
    assert len(annotation_paths) == 14
    assert unique_tag_seqs == {("a", "b"), ("a", "b", "c")}


def test_index_same_location_twice(ldb_instance, data_dir):
    path = os.fspath(data_dir / "fashion-mnist" / "original")
    ret1 = main(["index", "-m", "bare", "--add-tag=img", path])
    paths1 = get_indexed_data_paths(ldb_instance)
    data_object_meta1 = load_data_file(paths1[0][0])

    ret2 = main(["index", "-m", "bare", path])
    paths2 = get_indexed_data_paths(ldb_instance)
    data_object_meta2 = load_data_file(paths1[0][0])
    tag_seqs = get_obj_tags(paths2[0])
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
    assert tag_seqs == [["img"]] * len(tag_seqs)


def test_index_annotation_file(ldb_instance, data_dir):
    path = os.fspath(
        data_dir / "fashion-mnist/original/has_both/train/00002.json",
    )
    ret = main(["index", "-m", "bare", "--add-tag=img", path])
    (
        data_object_meta_paths,
        annotation_meta_paths,
        annotation_paths,
    ) = get_indexed_data_paths(ldb_instance)
    tag_seqs = get_obj_tags(data_object_meta_paths)

    assert ret == 0
    assert len(data_object_meta_paths) == 1
    assert len(annotation_meta_paths) == 1
    assert len(annotation_paths) == 1
    assert tag_seqs == [["img"]] * len(tag_seqs)


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
    tag_seqs = get_obj_tags(data_object_meta_paths)

    assert ret == 0
    assert len(data_object_meta_paths) == 0
    assert len(annotation_meta_paths) == 0
    assert len(annotation_paths) == 0
    assert tag_seqs == [["img"]] * len(tag_seqs)


def test_index_data_object_file(ldb_instance, data_dir):
    path = os.fspath(
        data_dir / "fashion-mnist/original/has_both/train/00002.png",
    )
    ret = main(["index", "-m", "bare", "--add-tag=img", path])
    (
        data_object_meta_paths,
        annotation_meta_paths,
        annotation_paths,
    ) = get_indexed_data_paths(ldb_instance)
    tag_seqs = get_obj_tags(data_object_meta_paths)

    assert ret == 0
    assert len(data_object_meta_paths) == 1
    assert len(annotation_meta_paths) == 1
    assert len(annotation_paths) == 1
    assert tag_seqs == [["img"]] * len(tag_seqs)


def test_index_annotation_file_glob(ldb_instance, data_dir):
    path = os.fspath(data_dir / "fashion-mnist/original/*/t*/000[0-1]*.json")
    ret = main(["index", "-m", "bare", "--add-tag=img", path])
    (
        data_object_meta_paths,
        annotation_meta_paths,
        annotation_paths,
    ) = get_indexed_data_paths(ldb_instance)
    tag_seqs = get_obj_tags(data_object_meta_paths)

    assert ret == 0
    assert len(data_object_meta_paths) == 10
    assert len(annotation_meta_paths) == 10
    assert len(annotation_paths) == 6
    assert tag_seqs == [["img"]] * len(tag_seqs)


def test_index_glob_dir_path(ldb_instance, data_dir):
    path = os.fspath(data_dir / "fashion-mnist/original/*/t*/")
    ret = main(["index", "-m", "bare", "--add-tag=img", path])
    (
        data_object_meta_paths,
        annotation_meta_paths,
        annotation_paths,
    ) = get_indexed_data_paths(ldb_instance)
    tag_seqs = get_obj_tags(data_object_meta_paths)

    assert ret == 0
    assert len(data_object_meta_paths) == 23
    assert len(annotation_meta_paths) == 23
    assert len(annotation_paths) == 10
    assert tag_seqs == [["img"]] * len(tag_seqs)


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
    tag_seqs = get_obj_tags(data_object_meta_paths)
    assert ret == 0
    assert len(data_object_meta_paths) == 1
    assert len(annotation_meta_paths) == 1
    assert len(annotation_paths) == 1
    assert tag_seqs == [[]] * len(tag_seqs)


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

    ret = main(["index", "-m", "bare", "--add-tags=img", storage_path])

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
    tag_seqs = get_obj_tags(data_object_meta_paths)

    assert ret == 0
    assert len(data_object_meta_paths) == 23
    assert len(annotation_meta_paths) == 23
    assert len(annotation_paths) == 10
    assert non_data_object_meta == []
    assert non_annotation_meta == []
    assert non_annotation == []
    assert num_read_add_data_object_files == 23
    assert num_read_add_annotation_files == 23
    assert tag_seqs == [["img"]] * len(tag_seqs)


def test_index_relative_path(ldb_instance, data_dir):
    path = os.fspath(data_dir / "fashion-mnist" / "original")
    with chdir(path):
        ret = main(["index", "-m", "bare", "--add-tag=img", "."])

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
    tag_seqs = get_obj_tags(data_object_meta_paths)

    assert ret == 0
    assert len(data_object_meta_paths) == 32
    assert len(annotation_meta_paths) == 23
    assert len(annotation_paths) == 10
    assert non_data_object_meta == []
    assert non_annotation_meta == []
    assert non_annotation == []
    assert tag_seqs == [["img"]] * len(tag_seqs)


def test_index_strict(ldb_instance, data_dir):
    path = os.fspath(data_dir / "fashion-mnist" / "original")
    ret = main(["index", "-m", "strict", "--add-tags=img", path])
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
    tag_seqs = get_obj_tags(data_object_meta_paths)

    assert ret == 0
    assert len(data_object_meta_paths) == 23
    assert len(annotation_meta_paths) == 23
    assert len(annotation_paths) == 10
    assert non_data_object_meta == []
    assert non_annotation_meta == []
    assert non_annotation == []
    assert tag_seqs == [["img"]] * len(tag_seqs)


def test_index_annotation_only(ldb_instance, data_dir):
    main(
        [
            "index",
            "-m",
            "bare",
            "--add-tag=test",
            os.fspath(data_dir / "data-object-only"),
        ],
    )
    ret = main(
        [
            "index",
            "-m",
            "annot",
            "--add-tag=img",
            os.fspath(data_dir / "annotation-only"),
        ],
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
    tag_seqs = get_obj_tags(data_object_meta_paths)
    unique_tag_seqs = set(map(tuple, tag_seqs))  # type: ignore[arg-type]

    assert ret == 0
    assert len(data_object_meta_paths) == 32
    assert len(annotation_meta_paths) == 29
    assert len(annotation_paths) == 16
    assert non_data_object_meta == []
    assert non_annotation_meta == []
    assert non_annotation == []
    assert unique_tag_seqs == {("test",), ("img", "test")}


def test_index_inferred(ldb_instance, data_dir):
    ret = main(
        [
            "index",
            "-m",
            "infer",
            "--add-tag=img",
            os.fspath(data_dir / "inferred/multilevel"),
        ],
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
    tag_seqs = get_obj_tags(data_object_meta_paths)

    annot_hashes = get_current_annotation_hashes(
        ldb_instance,
        [
            "31ed21a2633c6802e756dd06220b0b82",
            "def3cbcb30f3254a2a220e51ddf45375",
        ],
    )
    annotations = get_annotations(ldb_instance, annot_hashes)
    expected_annotations = [
        {"label": {"blue": {"2": "a"}}},
        {"label": {"red": "3"}},
    ]
    assert ret == 0
    assert len(data_object_meta_paths) == 23
    assert len(annotation_meta_paths) == 23
    assert len(annotation_paths) == 22
    assert non_data_object_meta == []
    assert non_annotation_meta == []
    assert non_annotation == []
    assert annotations == expected_annotations
    assert tag_seqs == [["img"]] * len(tag_seqs)

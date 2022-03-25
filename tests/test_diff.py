import os

import pytest

from ldb.diff import DiffItem, DiffType, diff
from ldb.main import main

from .utils import stage_new_workspace


@pytest.fixture
def workspace_diff_setup(tmp_path, ldb_instance, workspace_path, data_dir):
    filenames1 = [
        "00002.png",
        "00010.png",
        "00015.png",
        "00016.png",
        "00021.png",
        "00023.png",
        "00029.png",
    ]
    file_paths1 = [
        data_dir / "fashion-mnist/original/**" / f for f in filenames1
    ]
    filenames2 = [
        "00002.png",
        "00010.png",
        "00015.png",
        "00023.png",
        "00026.png",
        "00029.png",
    ]
    file_paths2 = [
        data_dir / "fashion-mnist/updates/**" / f for f in filenames2
    ]

    path = tmp_path / "workspace"
    os.chdir(path)

    main(["index", "-m", "bare"] + [os.fspath(f) for f in file_paths1])
    stage_new_workspace(path, "a")
    main(["add"] + [os.fspath(f) for f in file_paths1])
    main(["commit"])

    main(["index", "-m", "bare"] + [os.fspath(f) for f in file_paths2])
    stage_new_workspace(path, "b")
    main(["add"] + [os.fspath(f) for f in file_paths2])
    return [
        DiffItem(
            data_object_hash="2c4a9d28cc2ce780d17bea08d45d33b3",
            annotation_hash1="5ca184106560369a01db4fdfc3bbf5da",
            annotation_hash2="",
            diff_type=DiffType.DELETION,
            data_object_path=os.fspath(
                data_dir / "fashion-mnist/original/has_both/train/00016.png",
            ).replace(os.path.sep, "/"),
            annotation_version1=1,
            annotation_version2=0,
        ),
        DiffItem(
            data_object_hash="399146164375493f916025b04d00709c",
            annotation_hash1="97dde24d0e61ac83f051cd748e16f5dc",
            annotation_hash2="97dde24d0e61ac83f051cd748e16f5dc",
            diff_type=DiffType.SAME,
            data_object_path=os.fspath(
                data_dir / "fashion-mnist/updates/no_inference/00023.png",
            ).replace(os.path.sep, "/"),
            annotation_version1=1,
            annotation_version2=1,
        ),
        DiffItem(
            data_object_hash="47149106168f7d88fcea9e168608f129",
            annotation_hash1="97dde24d0e61ac83f051cd748e16f5dc",
            annotation_hash2="062133135568b9e077d15703593fb0e6",
            diff_type=DiffType.MODIFICATION,
            data_object_path=os.fspath(
                data_dir / "fashion-mnist/updates/same_inference/00029.png",
            ).replace(os.path.sep, "/"),
            annotation_version1=1,
            annotation_version2=2,
        ),
        DiffItem(
            data_object_hash="65383bee429980b89febc3f9b3349379",
            annotation_hash1="5bd583e12fd78ccc9dc61a36debd985f",
            annotation_hash2="5bd583e12fd78ccc9dc61a36debd985f",
            diff_type=DiffType.SAME,
            data_object_path=os.fspath(
                data_dir / "fashion-mnist/updates/no_inference/00010.png",
            ).replace(os.path.sep, "/"),
            annotation_version1=1,
            annotation_version2=1,
        ),
        DiffItem(
            data_object_hash="66e0373a2a989870fbc2c7791d8e6490",
            annotation_hash1="ef8b9794e2e24d461477fc6b847e8540",
            annotation_hash2="",
            diff_type=DiffType.DELETION,
            data_object_path=os.fspath(
                data_dir / "fashion-mnist/original/has_both/train/00021.png",
            ).replace(os.path.sep, "/"),
            annotation_version1=1,
            annotation_version2=0,
        ),
        DiffItem(
            data_object_hash="a2430513e897d5abcf62a55b8df81355",
            annotation_hash1="268daa854dde9f160c2b2ffe1d2ed74b",
            annotation_hash2="8d68100832b01b8b8470a14b467d2f63",
            diff_type=DiffType.MODIFICATION,
            data_object_path=os.fspath(
                data_dir / "fashion-mnist/updates/diff_inference/00015.png",
            ).replace(os.path.sep, "/"),
            annotation_version1=1,
            annotation_version2=2,
        ),
        DiffItem(
            data_object_hash="def3cbcb30f3254a2a220e51ddf45375",
            annotation_hash1="",
            annotation_hash2="",
            diff_type=DiffType.ADDITION,
            data_object_path=os.fspath(
                data_dir / "fashion-mnist/updates/no_inference/00026.png",
            ).replace(os.path.sep, "/"),
            annotation_version1=0,
            annotation_version2=0,
        ),
        DiffItem(
            data_object_hash="e299594dc1f79f8e69c6d79a42699822",
            annotation_hash1="46fa5381b9cd9433f03670ca9d7828dc",
            annotation_hash2="3ee7b8de6da6d440c43f7afecaf590ef",
            diff_type=DiffType.MODIFICATION,
            data_object_path=os.fspath(
                data_dir / "fashion-mnist/updates/diff_inference/00002.png",
            ).replace(os.path.sep, "/"),
            annotation_version1=1,
            annotation_version2=2,
        ),
    ]


def test_diff_two_datasets(ldb_instance, workspace_path, workspace_diff_setup):
    main(["commit"])
    diff_items = list(diff(ldb_instance, workspace_path, "ds:a", "ds:b"))
    assert diff_items == workspace_diff_setup


def test_diff_workspace_and_other_dataset(
    ldb_instance,
    workspace_path,
    workspace_diff_setup,
):
    diff_items = list(diff(ldb_instance, workspace_path, "ds:a"))
    assert diff_items == workspace_diff_setup


def test_cli_diff_summary(
    workspace_diff_setup,
    capsys,
):
    main(["commit"])
    capsys.readouterr()
    main(["diff", "ds:a", "ds:b", "-s"])
    assert len(capsys.readouterr().out.splitlines()) == 4

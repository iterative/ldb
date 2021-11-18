import os

import pytest

from ldb.diff import DiffItem, diff
from ldb.main import main
from ldb.stage import stage_workspace
from ldb.utils import current_time
from ldb.workspace import WorkspaceDataset


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
    stage_workspace(
        path,
        WorkspaceDataset(
            dataset_name="a",
            staged_time=current_time(),
            parent="",
            tags=[],
        ),
    )
    os.chdir(path)
    main(["add"] + [os.fspath(f) for f in file_paths1])
    main(["commit"])
    stage_workspace(
        path,
        WorkspaceDataset(
            dataset_name="b",
            staged_time=current_time(),
            parent="",
            tags=[],
        ),
    )
    main(["add"] + [os.fspath(f) for f in file_paths2])
    return [
        DiffItem(
            data_object_hash="2c4a9d28cc2ce780d17bea08d45d33b3",
            annotation_hash1="438def7684ae376b65cf522258d9aa8a",
            annotation_hash2="",
            data_object_path=os.fspath(
                data_dir / "fashion-mnist/original/has_both/train/00016.png",
            ).replace(os.path.sep, "/"),
            annotation_version1=1,
            annotation_version2=0,
        ),
        DiffItem(
            data_object_hash="399146164375493f916025b04d00709c",
            annotation_hash1="818e4e07715f01efd2b8b823163b910d",
            annotation_hash2="818e4e07715f01efd2b8b823163b910d",
            data_object_path=os.fspath(
                data_dir / "fashion-mnist/updates/no_inference/00023.png",
            ).replace(os.path.sep, "/"),
            annotation_version1=1,
            annotation_version2=1,
        ),
        DiffItem(
            data_object_hash="47149106168f7d88fcea9e168608f129",
            annotation_hash1="818e4e07715f01efd2b8b823163b910d",
            annotation_hash2="83c839bd3ca50c68cd17af5395d879d6",
            data_object_path=os.fspath(
                data_dir / "fashion-mnist/updates/same_inference/00029.png",
            ).replace(os.path.sep, "/"),
            annotation_version1=1,
            annotation_version2=2,
        ),
        DiffItem(
            data_object_hash="65383bee429980b89febc3f9b3349379",
            annotation_hash1="558d2a3f75f93be0b74a75a58b992403",
            annotation_hash2="558d2a3f75f93be0b74a75a58b992403",
            data_object_path=os.fspath(
                data_dir / "fashion-mnist/updates/no_inference/00010.png",
            ).replace(os.path.sep, "/"),
            annotation_version1=1,
            annotation_version2=1,
        ),
        DiffItem(
            data_object_hash="66e0373a2a989870fbc2c7791d8e6490",
            annotation_hash1="ea37760e357f44bf15d525022a5a87db",
            annotation_hash2="",
            data_object_path=os.fspath(
                data_dir / "fashion-mnist/original/has_both/train/00021.png",
            ).replace(os.path.sep, "/"),
            annotation_version1=1,
            annotation_version2=0,
        ),
        DiffItem(
            data_object_hash="a2430513e897d5abcf62a55b8df81355",
            annotation_hash1="a971780236ad55f28aa6248467f6e27f",
            annotation_hash2="89723aed8ac59ede5e9884956f8fa90a",
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
            data_object_path=os.fspath(
                data_dir / "fashion-mnist/updates/no_inference/00026.png",
            ).replace(os.path.sep, "/"),
            annotation_version1=0,
            annotation_version2=0,
        ),
        DiffItem(
            data_object_hash="e299594dc1f79f8e69c6d79a42699822",
            annotation_hash1="5c2be1dfbf9bc784a3e1bb42e1f2ccaf",
            annotation_hash2="cc0b2928a6401478688b7a382290c86a",
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

import os
import re
import shutil
from pathlib import Path
from typing import List, Optional, Sequence

from ldb import path
from ldb.core import add_default_read_add_storage
from ldb.ls import DatasetListing, ls, ls_collection
from ldb.main import main
from ldb.path import WorkspacePath
from ldb.utils import DATASET_PREFIX, ROOT, chdir
from ldb.workspace import collection_dir_to_object

from .utils import DATA_DIR, stage_new_workspace

UPDATES_DIR = path.join(DATA_DIR.as_posix(), "fashion-mnist", "updates")
ORIGINAL_DIR = path.join(DATA_DIR.as_posix(), "fashion-mnist", "original")
UPDATES_DIR_LISTINGS = [
    DatasetListing(
        data_object_hash="31ed21a2633c6802e756dd06220b0b82",
        data_object_path=path.join(UPDATES_DIR, "no_inference", "00028.png"),
        annotation_hash="",
        annotation_version=0,
    ),
    DatasetListing(
        data_object_hash="399146164375493f916025b04d00709c",
        data_object_path=path.join(UPDATES_DIR, "no_inference", "00023.png"),
        annotation_hash="818e4e07715f01efd2b8b823163b910d",
        annotation_version=1,
    ),
    DatasetListing(
        data_object_hash="47149106168f7d88fcea9e168608f129",
        data_object_path=path.join(UPDATES_DIR, "same_inference", "00029.png"),
        annotation_hash="83c839bd3ca50c68cd17af5395d879d6",
        annotation_version=1,
    ),
    DatasetListing(
        data_object_hash="65383bee429980b89febc3f9b3349379",
        data_object_path=path.join(UPDATES_DIR, "no_inference", "00010.png"),
        annotation_hash="",
        annotation_version=0,
    ),
    DatasetListing(
        data_object_hash="66e0373a2a989870fbc2c7791d8e6490",
        data_object_path=path.join(UPDATES_DIR, "no_inference", "00021.png"),
        annotation_hash="ea37760e357f44bf15d525022a5a87db",
        annotation_version=1,
    ),
    DatasetListing(
        data_object_hash="a2430513e897d5abcf62a55b8df81355",
        data_object_path=path.join(UPDATES_DIR, "diff_inference", "00015.png"),
        annotation_hash="89723aed8ac59ede5e9884956f8fa90a",
        annotation_version=1,
    ),
    DatasetListing(
        data_object_hash="b5fba326c8247d9e62aa17a109146c02",
        data_object_path=path.join(UPDATES_DIR, "same_inference", "00040.png"),
        annotation_hash="131ce3a7527d7d2cbc843092bd7f1bd0",
        annotation_version=1,
    ),
    DatasetListing(
        data_object_hash="def3cbcb30f3254a2a220e51ddf45375",
        data_object_path=path.join(UPDATES_DIR, "no_inference", "00026.png"),
        annotation_hash="",
        annotation_version=0,
    ),
    DatasetListing(
        data_object_hash="e299594dc1f79f8e69c6d79a42699822",
        data_object_path=path.join(UPDATES_DIR, "diff_inference", "00002.png"),
        annotation_hash="cc0b2928a6401478688b7a382290c86a",
        annotation_version=1,
    ),
]


def is_hash(s: str) -> bool:
    return re.fullmatch("[a-f0-9]{32}", s) is not None


def sorted_ls(
    ldb_dir: Path,
    paths: Sequence[str],
    query: Optional[str] = None,
) -> List[DatasetListing]:
    return sorted(
        ls(ldb_dir, paths, query),
        key=lambda d: (
            d.data_object_hash,
            d.annotation_hash,
            d.annotation_version,
            d.data_object_path,
        ),
    )


def test_ls_storage_locations(ldb_instance, workspace_path, data_dir):
    loc1 = data_dir / "fashion-mnist/updates/diff_inference"
    loc2 = data_dir / "fashion-mnist/updates/no_inference"
    loc_strings = [os.fspath(loc1), os.fspath(loc2)]
    main(["index", "-f", "bare", *loc_strings])
    listings = sorted_ls(ldb_instance, loc_strings)
    expected = [
        DatasetListing(
            data_object_hash="31ed21a2633c6802e756dd06220b0b82",
            data_object_path=path.join(
                UPDATES_DIR,
                "no_inference",
                "00028.png",
            ),
            annotation_hash="",
            annotation_version=0,
        ),
        DatasetListing(
            data_object_hash="399146164375493f916025b04d00709c",
            data_object_path=path.join(
                UPDATES_DIR,
                "no_inference",
                "00023.png",
            ),
            annotation_hash="818e4e07715f01efd2b8b823163b910d",
            annotation_version=1,
        ),
        DatasetListing(
            data_object_hash="65383bee429980b89febc3f9b3349379",
            data_object_path=path.join(
                UPDATES_DIR,
                "no_inference",
                "00010.png",
            ),
            annotation_hash="",
            annotation_version=0,
        ),
        DatasetListing(
            data_object_hash="66e0373a2a989870fbc2c7791d8e6490",
            data_object_path=path.join(
                UPDATES_DIR,
                "no_inference",
                "00021.png",
            ),
            annotation_hash="ea37760e357f44bf15d525022a5a87db",
            annotation_version=1,
        ),
        DatasetListing(
            data_object_hash="a2430513e897d5abcf62a55b8df81355",
            data_object_path=path.join(
                UPDATES_DIR,
                "diff_inference",
                "00015.png",
            ),
            annotation_hash="89723aed8ac59ede5e9884956f8fa90a",
            annotation_version=1,
        ),
        DatasetListing(
            data_object_hash="def3cbcb30f3254a2a220e51ddf45375",
            data_object_path=path.join(
                UPDATES_DIR,
                "no_inference",
                "00026.png",
            ),
            annotation_hash="",
            annotation_version=0,
        ),
        DatasetListing(
            data_object_hash="e299594dc1f79f8e69c6d79a42699822",
            data_object_path=path.join(
                UPDATES_DIR,
                "diff_inference",
                "00002.png",
            ),
            annotation_hash="cc0b2928a6401478688b7a382290c86a",
            annotation_version=1,
        ),
    ]
    assert listings == expected


def test_ls_data_objects(ldb_instance, workspace_path, index_original):
    main(
        [
            "add",
            "0x3c679fd1b8537dc7da1272a085e388e6",
            "0x982814b9116dce7882dfc31636c3ff7a",
            "0xebbc6c0cebb66738942ee56513f9ee2f",
            "0x1e0759182b328fd22fcdb5e6beb54adf",
        ],
    )
    listings = sorted_ls(ldb_instance, [])
    expected = [
        DatasetListing(
            data_object_hash="1e0759182b328fd22fcdb5e6beb54adf",
            data_object_path=path.join(
                ORIGINAL_DIR,
                "data_objects_only",
                "00014.png",
            ),
            annotation_hash="",
            annotation_version=0,
        ),
        DatasetListing(
            data_object_hash="3c679fd1b8537dc7da1272a085e388e6",
            data_object_path=path.join(
                ORIGINAL_DIR,
                "data_objects_only",
                "00036.png",
            ),
            annotation_hash="",
            annotation_version=0,
        ),
        DatasetListing(
            data_object_hash="982814b9116dce7882dfc31636c3ff7a",
            data_object_path=path.join(
                ORIGINAL_DIR,
                "has_both",
                "test",
                "00025.png",
            ),
            annotation_hash="818e4e07715f01efd2b8b823163b910d",
            annotation_version=1,
        ),
        DatasetListing(
            data_object_hash="ebbc6c0cebb66738942ee56513f9ee2f",
            data_object_path=path.join(
                ORIGINAL_DIR,
                "has_both",
                "test",
                "00047.png",
            ),
            annotation_hash="a971780236ad55f28aa6248467f6e27f",
            annotation_version=1,
        ),
    ]
    assert listings == expected


def test_ls_datasets(ldb_instance, ds_a, ds_b):
    listings = sorted_ls(ldb_instance, [ds_a, ds_b])
    expected = [
        DatasetListing(
            data_object_hash="1e0759182b328fd22fcdb5e6beb54adf",
            data_object_path=path.join(
                ORIGINAL_DIR,
                "data_objects_only",
                "00014.png",
            ),
            annotation_hash="",
            annotation_version=0,
        ),
        DatasetListing(
            data_object_hash="2f3533f1e35349602fbfaf0ec9b3ef3f",
            data_object_path=path.join(
                ORIGINAL_DIR,
                "data_objects_only",
                "00017.png",
            ),
            annotation_hash="",
            annotation_version=0,
        ),
        DatasetListing(
            data_object_hash="3c679fd1b8537dc7da1272a085e388e6",
            data_object_path=path.join(
                ORIGINAL_DIR,
                "data_objects_only",
                "00036.png",
            ),
            annotation_hash="",
            annotation_version=0,
        ),
        DatasetListing(
            data_object_hash="95789bb1ac140460cefc97a6e66a9ee8",
            data_object_path=path.join(
                ORIGINAL_DIR,
                "has_both",
                "train",
                "00007.png",
            ),
            annotation_hash="a971780236ad55f28aa6248467f6e27f",
            annotation_version=1,
        ),
        DatasetListing(
            data_object_hash="982814b9116dce7882dfc31636c3ff7a",
            data_object_path=path.join(
                ORIGINAL_DIR,
                "has_both",
                "test",
                "00025.png",
            ),
            annotation_hash="818e4e07715f01efd2b8b823163b910d",
            annotation_version=1,
        ),
        DatasetListing(
            data_object_hash="e1c3ef93e4e1cf108fa2a4c9d6e03af2",
            data_object_path=path.join(
                ORIGINAL_DIR,
                "data_objects_only",
                "00011.png",
            ),
            annotation_hash="",
            annotation_version=0,
        ),
        DatasetListing(
            data_object_hash="ebbc6c0cebb66738942ee56513f9ee2f",
            data_object_path=path.join(
                ORIGINAL_DIR,
                "has_both",
                "test",
                "00047.png",
            ),
            annotation_hash="a971780236ad55f28aa6248467f6e27f",
            annotation_version=1,
        ),
    ]
    assert listings == expected


def test_ls_root_dataset(ldb_instance, data_dir):
    main(
        ["index", "-f", "bare", os.fspath(data_dir / "fashion-mnist/updates")],
    )
    listings = sorted_ls(ldb_instance, [f"{DATASET_PREFIX}{ROOT}"])
    assert listings == UPDATES_DIR_LISTINGS


def test_ls_root_dataset_query(ldb_instance, data_dir):
    main(
        ["index", "-f", "bare", os.fspath(data_dir / "fashion-mnist/updates")],
    )
    listings = sorted_ls(
        ldb_instance,
        [f"{DATASET_PREFIX}{ROOT}"],
        "@ == `null` || label == inference.label || label == `3`",
    )
    expected = [
        DatasetListing(
            data_object_hash="31ed21a2633c6802e756dd06220b0b82",
            data_object_path=path.join(
                UPDATES_DIR,
                "no_inference",
                "00028.png",
            ),
            annotation_hash="",
            annotation_version=0,
        ),
        DatasetListing(
            data_object_hash="47149106168f7d88fcea9e168608f129",
            data_object_path=path.join(
                UPDATES_DIR,
                "same_inference",
                "00029.png",
            ),
            annotation_hash="83c839bd3ca50c68cd17af5395d879d6",
            annotation_version=1,
        ),
        DatasetListing(
            data_object_hash="65383bee429980b89febc3f9b3349379",
            data_object_path=path.join(
                UPDATES_DIR,
                "no_inference",
                "00010.png",
            ),
            annotation_hash="",
            annotation_version=0,
        ),
        DatasetListing(
            data_object_hash="66e0373a2a989870fbc2c7791d8e6490",
            data_object_path=path.join(
                UPDATES_DIR,
                "no_inference",
                "00021.png",
            ),
            annotation_hash="ea37760e357f44bf15d525022a5a87db",
            annotation_version=1,
        ),
        DatasetListing(
            data_object_hash="b5fba326c8247d9e62aa17a109146c02",
            data_object_path=path.join(
                UPDATES_DIR,
                "same_inference",
                "00040.png",
            ),
            annotation_hash="131ce3a7527d7d2cbc843092bd7f1bd0",
            annotation_version=1,
        ),
        DatasetListing(
            data_object_hash="def3cbcb30f3254a2a220e51ddf45375",
            data_object_path=path.join(
                UPDATES_DIR,
                "no_inference",
                "00026.png",
            ),
            annotation_hash="",
            annotation_version=0,
        ),
    ]
    assert listings == expected


def test_ls_current_workspace(workspace_path, data_dir, ldb_instance):
    main(
        ["index", "-f", "bare", os.fspath(data_dir / "fashion-mnist/updates")],
    )
    add_default_read_add_storage(ldb_instance)
    shutil.copytree(
        data_dir / "fashion-mnist/updates",
        "./updates",
    )
    listings = sorted_ls(ldb_instance, ["."])
    assert listings == UPDATES_DIR_LISTINGS


def test_ls_another_workspace(
    workspace_path,
    data_dir,
    ldb_instance,
    tmp_path,
):
    other_workspace_path = tmp_path / "other-workspace"
    main(
        ["index", "-f", "bare", os.fspath(data_dir / "fashion-mnist/updates")],
    )
    stage_new_workspace(other_workspace_path)
    with chdir(other_workspace_path):
        main(["add", f"{DATASET_PREFIX}{ROOT}"])
    with chdir(workspace_path):
        listings = sorted_ls(ldb_instance, [os.fspath(other_workspace_path)])
    assert listings == UPDATES_DIR_LISTINGS


def test_ls_collection_with_workspace_dataset(
    tmp_path,
    data_dir,
    ldb_instance,
):
    workspace_path = tmp_path / "workspace"
    dirs_to_add = [
        data_dir / "fashion-mnist/original/has_both/train",
        data_dir / "fashion-mnist/original/data_objects_only/00011.png",
        data_dir / "fashion-mnist/updates",
    ]
    for dir_path in dirs_to_add:
        main(["index", "-f", "bare", os.fspath(dir_path)])
    stage_new_workspace(workspace_path)
    with chdir(workspace_path):
        main(["add", f"{DATASET_PREFIX}{ROOT}"])
        ws_collection = collection_dir_to_object(
            workspace_path / WorkspacePath.COLLECTION,
        )
        ds_listings = ls_collection(ldb_instance, ws_collection)
    annot_versions = [d.annotation_version for d in ds_listings]
    # fsspec's LocalFileSystem._strip_protocol does some normalization during
    # indexing, so we cast everything to Path objects for comparison
    paths = [Path(d.data_object_path) for d in ds_listings]

    expected_annot_versions = [1, 1, 1, 2, 1, 1, 1, 1, 2, 2, 1, 1, 0, 2]
    expected_str_paths = [
        "original/has_both/train/00016.png",
        "updates/no_inference/00028.png",
        "updates/no_inference/00023.png",
        "updates/same_inference/00029.png",
        "updates/no_inference/00010.png",
        "updates/no_inference/00021.png",
        "original/has_both/train/00038.png",
        "original/has_both/train/00007.png",
        "updates/diff_inference/00015.png",
        "updates/same_inference/00040.png",
        "original/has_both/train/00033.png",
        "updates/no_inference/00026.png",
        "original/data_objects_only/00011.png",
        "updates/diff_inference/00002.png",
    ]
    expected_paths = [
        data_dir / "fashion-mnist" / p for p in expected_str_paths
    ]
    assert annot_versions == expected_annot_versions
    assert paths == expected_paths
    assert all(is_hash(d.data_object_hash) for d in ds_listings)
    assert all(
        is_hash(d.annotation_hash)
        if d.annotation_version
        else d.annotation_hash == ""
        for d in ds_listings
    )

import os
import shutil

from ldb.core import add_default_read_add_storage
from ldb.evaluate import evaluate
from ldb.main import main
from ldb.utils import DATASET_PREFIX, ROOT, chdir

from .utils import stage_new_workspace


def test_evaluate_storage_location(ldb_instance, data_dir):
    dir_to_eval = data_dir / "fashion-mnist/updates"
    main(["add", os.fspath(dir_to_eval)])
    result = dict(
        evaluate(
            ldb_instance,
            "[label, inference.label]",
            [os.fspath(dir_to_eval)],
        ),
    )
    expected = {
        "e299594dc1f79f8e69c6d79a42699822": [0, 1],
        "a2430513e897d5abcf62a55b8df81355": [7, 1],
        "65383bee429980b89febc3f9b3349379": None,
        "66e0373a2a989870fbc2c7791d8e6490": [3, None],
        "399146164375493f916025b04d00709c": [4, None],
        "def3cbcb30f3254a2a220e51ddf45375": None,
        "31ed21a2633c6802e756dd06220b0b82": None,
        "47149106168f7d88fcea9e168608f129": [4, 4],
        "b5fba326c8247d9e62aa17a109146c02": [6, 6],
    }
    assert result == expected


def test_evaluate_data_objects(ldb_instance, data_dir):
    main(["index", os.fspath(data_dir / "fashion-mnist/updates")])
    result = dict(
        evaluate(
            ldb_instance,
            "[label, inference.label]",
            [
                "0xa2430513e897d5abcf62a55b8df81355",
                "0x66e0373a2a989870fbc2c7791d8e6490",
                "0xdef3cbcb30f3254a2a220e51ddf45375",
                "0x47149106168f7d88fcea9e168608f129",
            ],
        ),
    )
    expected = {
        "a2430513e897d5abcf62a55b8df81355": [7, 1],
        "66e0373a2a989870fbc2c7791d8e6490": [3, None],
        "def3cbcb30f3254a2a220e51ddf45375": None,
        "47149106168f7d88fcea9e168608f129": [4, 4],
    }
    assert result == expected


def test_evaluate_datasets(ldb_instance, workspace_path, data_dir):
    dir_to_add = data_dir / "fashion-mnist/updates"
    main(["index", os.fspath(dir_to_add)])
    main(["stage", "ds:a"])
    main(
        [
            "add",
            "0xa2430513e897d5abcf62a55b8df81355",
            "0x66e0373a2a989870fbc2c7791d8e6490",
            "0xdef3cbcb30f3254a2a220e51ddf45375",
            "0x47149106168f7d88fcea9e168608f129",
        ],
    )
    main(["commit"])
    main(["stage", "ds:b"])
    main(
        [
            "add",
            "0xe299594dc1f79f8e69c6d79a42699822",
            "0xa2430513e897d5abcf62a55b8df81355",
            "0x65383bee429980b89febc3f9b3349379",
            "0x399146164375493f916025b04d00709c",
            "0xdef3cbcb30f3254a2a220e51ddf45375",
            "0xb5fba326c8247d9e62aa17a109146c02",
        ],
    )
    main(["commit"])
    result = dict(
        evaluate(
            ldb_instance,
            "[label, inference.label]",
            ["ds:a", "ds:b"],
        ),
    )
    expected = {
        "e299594dc1f79f8e69c6d79a42699822": [0, 1],
        "a2430513e897d5abcf62a55b8df81355": [7, 1],
        "65383bee429980b89febc3f9b3349379": None,
        "66e0373a2a989870fbc2c7791d8e6490": [3, None],
        "399146164375493f916025b04d00709c": [4, None],
        "def3cbcb30f3254a2a220e51ddf45375": None,
        "47149106168f7d88fcea9e168608f129": [4, 4],
        "b5fba326c8247d9e62aa17a109146c02": [6, 6],
    }
    assert result == expected


def test_evaluate_root_dataset(ldb_instance, data_dir):
    main(
        [
            "index",
            os.fspath(data_dir / "fashion-mnist/original/has_both/train"),
            os.fspath(data_dir / "fashion-mnist/updates"),
        ],
    )
    result = dict(
        evaluate(
            ldb_instance,
            "[label, inference.label]",
            [f"{DATASET_PREFIX}{ROOT}"],
        ),
    )
    expected = {
        "b5fba326c8247d9e62aa17a109146c02": [6, 6],
        "751111c36f27e3668b9b043987c18386": [2, None],
        "d0346148afcebd9cfccc809359baa4d8": [6, None],
        "47149106168f7d88fcea9e168608f129": [4, 4],
        "31ed21a2633c6802e756dd06220b0b82": [2, None],
        "def3cbcb30f3254a2a220e51ddf45375": [3, None],
        "399146164375493f916025b04d00709c": [4, None],
        "66e0373a2a989870fbc2c7791d8e6490": [3, None],
        "2c4a9d28cc2ce780d17bea08d45d33b3": [9, None],
        "a2430513e897d5abcf62a55b8df81355": [7, 1],
        "65383bee429980b89febc3f9b3349379": [5, None],
        "95789bb1ac140460cefc97a6e66a9ee8": [7, None],
        "e299594dc1f79f8e69c6d79a42699822": [0, 1],
    }
    assert result == expected


def test_evaluate_current_workspace(workspace_path, data_dir, ldb_instance):
    main(["index", os.fspath(data_dir / "fashion-mnist/updates")])
    add_default_read_add_storage(ldb_instance)
    shutil.copytree(
        data_dir / "fashion-mnist/updates/diff_inference",
        "./data1",
    )
    shutil.copytree(
        data_dir / "fashion-mnist/updates/same_inference",
        "./data2",
    )
    result = dict(
        evaluate(
            ldb_instance,
            "[label, inference.label]",
            ["."],
        ),
    )
    expected = {
        "e299594dc1f79f8e69c6d79a42699822": [0, 1],
        "a2430513e897d5abcf62a55b8df81355": [7, 1],
        "47149106168f7d88fcea9e168608f129": [4, 4],
        "b5fba326c8247d9e62aa17a109146c02": [6, 6],
    }
    assert result == expected


def test_evaluate_another_workspace(
    workspace_path,
    data_dir,
    ldb_instance,
    tmp_path,
):
    other_workspace_path = tmp_path / "other-workspace"
    stage_new_workspace(other_workspace_path)

    with chdir(other_workspace_path):
        main(
            ["add", os.fspath(data_dir / "fashion-mnist/updates")],
        )
    with chdir(workspace_path):
        result = dict(
            evaluate(
                ldb_instance,
                "[label, inference.label]",
                [os.fspath(other_workspace_path)],
            ),
        )
    expected = {
        "31ed21a2633c6802e756dd06220b0b82": None,
        "399146164375493f916025b04d00709c": [4, None],
        "47149106168f7d88fcea9e168608f129": [4, 4],
        "65383bee429980b89febc3f9b3349379": None,
        "66e0373a2a989870fbc2c7791d8e6490": [3, None],
        "a2430513e897d5abcf62a55b8df81355": [7, 1],
        "b5fba326c8247d9e62aa17a109146c02": [6, 6],
        "def3cbcb30f3254a2a220e51ddf45375": None,
        "e299594dc1f79f8e69c6d79a42699822": [0, 1],
    }
    assert result == expected

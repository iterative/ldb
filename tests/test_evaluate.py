import os
import shutil
from typing import Any, Dict, Sequence, Tuple

import pytest

from ldb.core import add_default_read_add_storage
from ldb.evaluate import evaluate
from ldb.main import main
from ldb.op_type import OpType
from ldb.typing import JSONDecoded
from ldb.utils import DATASET_PREFIX, ROOT, WORKSPACE_DATASET_PREFIX, chdir

from .data import QUERY_DATA
from .utils import is_data_object_meta_obj, stage_new_workspace

SIMPLE_MULTI_SELECT_QUERY = '[label, get(@, `"inference.label"`)]'


@pytest.mark.parametrize(
    "args,data_objs,annots",
    QUERY_DATA.values(),
    ids=QUERY_DATA.keys(),
)
def test_cli_eval_counts_root_dataset(
    args,
    data_objs,
    annots,
    fashion_mnist_session,
    capsys,
):
    ret = main(
        [
            "eval",
            f"{DATASET_PREFIX}{ROOT}",
            *args,
            "--query=@ != `null`",
        ],
    )
    out_lines = capsys.readouterr().out.splitlines()
    found_annots = sum(x == "true" for x in out_lines)
    found_data_objs = found_annots + sum(x == "false" for x in out_lines)
    assert ret == 0
    assert found_data_objs == data_objs
    assert found_annots == annots


@pytest.mark.parametrize(
    "indent_args,expected",
    [
        ([], '{\n  "inference": {\n    "label": 1\n  },\n  "label": 7\n}'),
        (
            ["--indent=2"],
            '{\n  "inference": {\n    "label": 1\n  },\n  "label": 7\n}',
        ),
        (["--indent=none"], '{"inference": {"label": 1}, "label": 7}'),
        (
            ["--indent=\t"],
            '{\n\t"inference": {\n\t\t"label": 1\n\t},\n\t"label": 7\n}',
        ),
    ],
    ids=["no-arg", "2", "none", "tab"],
)
def test_cli_eval_json_indent(
    indent_args,
    expected,
    fashion_mnist_session,
    capsys,
):
    main(
        [
            "eval",
            f"{DATASET_PREFIX}{ROOT}",
            "--query=@",
            "--json",
            *indent_args,
        ],
    )
    out_lines = capsys.readouterr().out
    assert expected in out_lines


@pytest.mark.parametrize(
    "args",
    [
        (),
        ("--limit", "10"),
        ("--query", "label"),
        ("--file", "fs.path"),
        ("--query", "label", "--file", "fs.path"),
        ("--limit", "4", "--query", "label", "--file", "fs.path"),
        ("--query", "label", "--limit", "4", "--file", "fs.path"),
    ],
)
def test_cli_eval_root_dataset(args, ldb_instance, data_dir):
    dir_to_eval = os.fspath(data_dir / "fashion-mnist/updates")
    main(["index", "-m", "bare", dir_to_eval])
    ret = main(["eval", f"{DATASET_PREFIX}{ROOT}", *args])
    assert ret == 0


def test_evaluate_storage_location(ldb_instance, data_dir):
    dir_to_eval = os.fspath(data_dir / "fashion-mnist/updates")
    main(["index", "-m", "bare", dir_to_eval])
    result = list(
        evaluate(
            ldb_instance,
            [dir_to_eval],
            [
                (
                    OpType.ANNOTATION_QUERY,
                    SIMPLE_MULTI_SELECT_QUERY,
                ),
            ],
        ),
    )
    expected = [
        ("31ed21a2633c6802e756dd06220b0b82", None),
        ("399146164375493f916025b04d00709c", [4, None]),
        ("47149106168f7d88fcea9e168608f129", [4, 4]),
        ("65383bee429980b89febc3f9b3349379", None),
        ("66e0373a2a989870fbc2c7791d8e6490", [3, None]),
        ("a2430513e897d5abcf62a55b8df81355", [7, 1]),
        ("b5fba326c8247d9e62aa17a109146c02", [6, 6]),
        ("def3cbcb30f3254a2a220e51ddf45375", None),
        ("e299594dc1f79f8e69c6d79a42699822", [0, 1]),
    ]
    assert result == expected


@pytest.mark.parametrize(
    "do_annotation_query,do_file_query",
    [
        (False, False),
        (False, True),
        (True, False),
        (True, True),
    ],
)
def test_evaluate_data_objects(
    ldb_instance,
    data_dir,
    do_annotation_query,
    do_file_query,
):
    query_args = []
    if do_annotation_query:
        query_args.append(
            (
                OpType.ANNOTATION_QUERY,
                SIMPLE_MULTI_SELECT_QUERY,
            ),
        )
    if do_file_query:
        query_args.append((OpType.FILE_QUERY, "@"))
    main(
        ["index", "-m", "bare", os.fspath(data_dir / "fashion-mnist/updates")],
    )
    result = evaluate(
        ldb_instance,
        [
            "id:a2430513e897d5abcf62a55b8df81355",
            "id:66e0373a2a989870fbc2c7791d8e6490",
            "id:def3cbcb30f3254a2a220e51ddf45375",
            "id:47149106168f7d88fcea9e168608f129",
        ],
        query_args,
    )
    result_columns = list(zip(*result))
    file_meta_result: Sequence[Dict[str, Any]] = ()
    annotation_result: Sequence[JSONDecoded] = ()

    expected_data_object_hashes: Tuple[str, ...] = (
        "47149106168f7d88fcea9e168608f129",
        "66e0373a2a989870fbc2c7791d8e6490",
        "a2430513e897d5abcf62a55b8df81355",
        "def3cbcb30f3254a2a220e51ddf45375",
    )
    if do_annotation_query and do_file_query:
        expected_data_object_hashes = expected_data_object_hashes[:3]
    expected_annotation_result: Sequence[JSONDecoded] = ()

    if do_file_query:
        file_meta_result = result_columns[1]
    else:
        annotation_result = result_columns[1]
        if do_annotation_query:
            expected_annotation_result = (
                [4, 4],
                [3, None],
                [7, 1],
                None,
            )
        else:
            expected_annotation_result = (
                {"label": 4, "inference": {"label": 4}},  # type: ignore[assignment] # noqa: E501
                {"label": 3},
                {"label": 7, "inference": {"label": 1}},
                None,
            )
    assert result_columns[0] == expected_data_object_hashes
    assert expected_annotation_result == annotation_result
    assert all(is_data_object_meta_obj(m) for m in file_meta_result)


def test_evaluate_datasets(ldb_instance, workspace_path, data_dir):
    dir_to_add = data_dir / "fashion-mnist/updates"
    main(["index", "-m", "bare", os.fspath(dir_to_add)])
    main(["stage", "ds:a"])
    main(
        [
            "add",
            "id:a2430513e897d5abcf62a55b8df81355",
            "id:66e0373a2a989870fbc2c7791d8e6490",
            "id:def3cbcb30f3254a2a220e51ddf45375",
            "id:47149106168f7d88fcea9e168608f129",
        ],
    )
    main(["commit"])
    main(["stage", "ds:b"])
    main(
        [
            "add",
            "id:e299594dc1f79f8e69c6d79a42699822",
            "id:a2430513e897d5abcf62a55b8df81355",
            "id:65383bee429980b89febc3f9b3349379",
            "id:399146164375493f916025b04d00709c",
            "id:def3cbcb30f3254a2a220e51ddf45375",
            "id:b5fba326c8247d9e62aa17a109146c02",
        ],
    )
    main(["commit"])
    result = list(
        evaluate(
            ldb_instance,
            ["ds:a", "ds:b"],
            [
                (
                    OpType.ANNOTATION_QUERY,
                    SIMPLE_MULTI_SELECT_QUERY,
                ),
            ],
        ),
    )
    expected = [
        ("399146164375493f916025b04d00709c", [4, None]),
        ("47149106168f7d88fcea9e168608f129", [4, 4]),
        ("65383bee429980b89febc3f9b3349379", None),
        ("66e0373a2a989870fbc2c7791d8e6490", [3, None]),
        ("a2430513e897d5abcf62a55b8df81355", [7, 1]),
        ("b5fba326c8247d9e62aa17a109146c02", [6, 6]),
        ("def3cbcb30f3254a2a220e51ddf45375", None),
        ("e299594dc1f79f8e69c6d79a42699822", [0, 1]),
    ]
    assert result == expected


@pytest.mark.parametrize(
    "limit",
    [0, 4],
)
def test_evaluate_root_dataset(limit, ldb_instance, data_dir):
    main(
        [
            "index",
            "-m",
            "bare",
            os.fspath(data_dir / "fashion-mnist/original/has_both/train"),
            os.fspath(data_dir / "fashion-mnist/updates"),
        ],
    )
    query_args = []
    if limit:
        query_args.append((OpType.LIMIT, limit))
    query_args.append(
        (
            OpType.ANNOTATION_QUERY,
            SIMPLE_MULTI_SELECT_QUERY,
        ),
    )
    result = list(
        evaluate(
            ldb_instance,
            [f"{DATASET_PREFIX}{ROOT}"],
            query_args,
        ),
    )
    expected = [
        ("2c4a9d28cc2ce780d17bea08d45d33b3", [9, None]),
        ("31ed21a2633c6802e756dd06220b0b82", [2, None]),
        ("399146164375493f916025b04d00709c", [4, None]),
        ("47149106168f7d88fcea9e168608f129", [4, 4]),
        ("65383bee429980b89febc3f9b3349379", [5, None]),
        ("66e0373a2a989870fbc2c7791d8e6490", [3, None]),
        ("751111c36f27e3668b9b043987c18386", [2, None]),
        ("95789bb1ac140460cefc97a6e66a9ee8", [7, None]),
        ("a2430513e897d5abcf62a55b8df81355", [7, 1]),
        ("b5fba326c8247d9e62aa17a109146c02", [6, 6]),
        ("d0346148afcebd9cfccc809359baa4d8", [6, None]),
        ("def3cbcb30f3254a2a220e51ddf45375", [3, None]),
        ("e299594dc1f79f8e69c6d79a42699822", [0, 1]),
    ]
    if limit:
        expected = expected[:limit]
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
    result = list(
        evaluate(
            ldb_instance,
            ["."],
            [(OpType.ANNOTATION_QUERY, "[label, inference.label]")],
        ),
    )
    expected = [
        ("47149106168f7d88fcea9e168608f129", [4, 4]),
        ("a2430513e897d5abcf62a55b8df81355", [7, 1]),
        ("b5fba326c8247d9e62aa17a109146c02", [6, 6]),
        ("e299594dc1f79f8e69c6d79a42699822", [0, 1]),
    ]
    assert result == expected


def test_evaluate_another_workspace(
    workspace_path,
    data_dir,
    ldb_instance,
    tmp_path,
):
    other_workspace_path = tmp_path / "other-workspace"
    stage_new_workspace(other_workspace_path)

    main(
        ["index", "-m", "bare", os.fspath(data_dir / "fashion-mnist/updates")],
    )
    with chdir(other_workspace_path):
        main(["add", f"{DATASET_PREFIX}{ROOT}"])
    ws_ident = f"{WORKSPACE_DATASET_PREFIX}{os.fspath(other_workspace_path)}"
    with chdir(workspace_path):
        result = list(
            evaluate(
                ldb_instance,
                [ws_ident],
                [
                    (
                        OpType.ANNOTATION_QUERY,
                        SIMPLE_MULTI_SELECT_QUERY,
                    ),
                ],
            ),
        )
    expected = [
        ("31ed21a2633c6802e756dd06220b0b82", None),
        ("399146164375493f916025b04d00709c", [4, None]),
        ("47149106168f7d88fcea9e168608f129", [4, 4]),
        ("65383bee429980b89febc3f9b3349379", None),
        ("66e0373a2a989870fbc2c7791d8e6490", [3, None]),
        ("a2430513e897d5abcf62a55b8df81355", [7, 1]),
        ("b5fba326c8247d9e62aa17a109146c02", [6, 6]),
        ("def3cbcb30f3254a2a220e51ddf45375", None),
        ("e299594dc1f79f8e69c6d79a42699822", [0, 1]),
    ]
    assert result == expected

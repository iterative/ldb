import os

from ldb.main import main
from ldb.status import WorkspaceStatus, status
from ldb.utils import chdir


def test_status_added_storage_location(
    data_dir,
    fashion_mnist_session,
    global_workspace_path,
):
    ldb_instance = fashion_mnist_session
    dir_to_add = os.fspath(data_dir / "fashion-mnist/original")
    with chdir(global_workspace_path):
        main(["add", dir_to_add])
        ws_status = status(ldb_instance, "")
    expected_ws_status = WorkspaceStatus(
        dataset_name="my-dataset",
        dataset_version=0,
        num_data_objects=32,
        num_annotations=23,
        auto_pull=False,
    )
    assert ws_status == expected_ws_status


def test_cli_status_added_storage_location(
    data_dir,
    fashion_mnist_session,
    global_workspace_path,
    capsys,
):
    dir_to_add = os.fspath(data_dir / "fashion-mnist/original")
    with chdir(global_workspace_path):
        with capsys.disabled():
            main(["add", dir_to_add])
        main(["status"])
        captured1 = capsys.readouterr().out
        with capsys.disabled():
            main(["commit", "-m", dir_to_add, "--auto-pull"])
        main(["status"])
        captured2 = capsys.readouterr().out
        main(["status", "ds:my-dataset"])
    captured3 = capsys.readouterr().out
    expected1 = (
        "Workspace directory: '.'\n"
        "\n"
        "ds:my-dataset\n"
        "  Num data objects:       32\n"
        "  Num annotations:        23\n"
        "\n"
        "  auto-pull:        false\n"
        "\n"
        "Unsaved changes:\n"
        "  Additions (+):       32\n"
        "  Deletions (-):        0\n"
        "  Modifications (m):    0\n"
    )
    expected2 = (
        "Workspace directory: '.'\n"
        "\n"
        "ds:my-dataset\n"
        "  Num data objects:       32\n"
        "  Num annotations:        23\n"
        "\n"
        "  auto-pull:        true\n"
        "\n"
        "No unsaved changes.\n"
    )
    expected3 = (
        f'ds:my-dataset.v1  "{dir_to_add}"\n'
        "  Num data objects:       32\n"
        "  Num annotations:        23\n"
        "\n"
        "  auto-pull:        true\n"
    )
    assert captured1 == expected1
    assert captured2 == expected2
    assert captured3 == expected3

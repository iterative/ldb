import getpass
import os

from ldb.dataset import CommitInfo, Dataset, DatasetVersion
from ldb.main import main
from ldb.path import InstanceDir
from ldb.stage import stage_new_workspace
from ldb.utils import current_time, load_data_file


def test_commit_new_dataset(tmp_path, data_dir, ldb_instance):
    workspace_path = tmp_path / "workspace"
    dir_to_add = data_dir / "fashion-mnist/original"
    stage_new_workspace("my-dataset", workspace_path)
    os.chdir(workspace_path)
    main(["add", f"{os.fspath(dir_to_add)}"])
    ret = main(["commit", "create a new dataset"])

    collection_file_paths = list(
        (ldb_instance / InstanceDir.COLLECTIONS).glob("*/*"),
    )
    dataset_version_file_paths = list(
        (ldb_instance / InstanceDir.DATASET_VERSIONS).glob("*/*"),
    )
    dataset_file_paths = list((ldb_instance / InstanceDir.DATASETS).glob("*"))

    collection_obj = load_data_file(collection_file_paths[0])
    dataset_version_obj = DatasetVersion.parse(
        load_data_file(dataset_version_file_paths[0]),
    )
    dataset_obj = Dataset.parse(load_data_file(dataset_file_paths[0]))
    collection_hash = (
        collection_file_paths[0].parent.name + collection_file_paths[0].name
    )
    dataset_version_hash = (
        dataset_version_file_paths[0].parent.name
        + dataset_version_file_paths[0].name
    )

    # skip checking the timestamps
    curr_time = current_time()
    dataset_version_obj.commit_info.commit_time = curr_time
    dataset_obj.created = curr_time

    expected_username = getpass.getuser()
    expected_dataset_version_obj = DatasetVersion(
        version=1,
        parent=None,
        collection=collection_hash,
        tags=[],
        commit_info=CommitInfo(
            created_by=expected_username,
            commit_time=curr_time,
            commit_message="create a new dataset",
        ),
    )
    expected_dataset_obj = Dataset(
        name="my-dataset",
        created_by=expected_username,
        created=curr_time,
        versions=[dataset_version_hash],
    )

    assert ret == 0
    assert len(collection_file_paths) == 1
    assert len(dataset_version_file_paths) == 1
    assert len(dataset_file_paths) == 1
    assert len(collection_obj) == 32
    assert sum(bool(a) for a in collection_obj.values()) == 23
    assert dataset_obj == expected_dataset_obj
    assert dataset_version_obj == expected_dataset_version_obj


def test_commit_multiple_versions(tmp_path, data_dir, ldb_instance):
    workspace_path = tmp_path / "workspace"
    paths = [
        data_dir / "fashion-mnist/original/has_both/test",
        data_dir / "fashion-mnist/original/has_both/train/000[01]*",
        data_dir / "fashion-mnist/updates",
    ]
    stage_new_workspace("my-dataset", workspace_path)
    os.chdir(workspace_path)
    for path in paths:
        main(["add", f"{os.fspath(path)}"])
        main(
            [
                "commit",
                f"add {os.fspath(path.relative_to(data_dir))}",
            ],
        )

    collection_file_paths = list(
        (ldb_instance / InstanceDir.COLLECTIONS).glob("*/*"),
    )
    dataset_version_file_paths = list(
        (ldb_instance / InstanceDir.DATASET_VERSIONS).glob("*/*"),
    )
    dataset_file_paths = list((ldb_instance / InstanceDir.DATASETS).glob("*"))
    for path_list in collection_file_paths, dataset_version_file_paths:
        path_list.sort(key=lambda p: p.stat().st_mtime)

    dataset_obj = Dataset.parse(load_data_file(dataset_file_paths[0]))

    dataset_version_hashes = [
        p.parent.name + p.name for p in dataset_version_file_paths
    ]
    dataset_version_objects = [
        DatasetVersion.parse(load_data_file(p))
        for p in dataset_version_file_paths
    ]

    commit_times = [d.commit_info.commit_time for d in dataset_version_objects]
    expected_messages = [
        "add fashion-mnist/original/has_both/test",
        "add fashion-mnist/original/has_both/train/000[01]*",
        "add fashion-mnist/updates",
    ]
    expected_dataset_obj = Dataset(
        name="my-dataset",
        created_by=getpass.getuser(),
        created=commit_times[0],
        versions=dataset_version_hashes,
    )

    assert len(dataset_file_paths) == 1
    assert [p.parent.name + p.name for p in collection_file_paths] == [
        d.collection for d in dataset_version_objects
    ]
    assert commit_times[0] < commit_times[1] < commit_times[2]
    assert [
        d.commit_info.commit_message for d in dataset_version_objects
    ] == expected_messages
    assert dataset_obj == expected_dataset_obj

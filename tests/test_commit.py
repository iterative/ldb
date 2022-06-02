import getpass
import os
from pathlib import Path

from ldb.dataset import CommitInfo, Dataset, DatasetVersion
from ldb.main import main
from ldb.path import InstanceDir
from ldb.utils import current_time, load_data_file


def test_commit_new_dataset(
    data_dir,
    ldb_instance,
    workspace_path,
    transform_infos,
):
    dir_to_add = os.fspath(data_dir / "fashion-mnist/original")
    main(["index", "-m", "bare", dir_to_add])
    main(["add", dir_to_add])
    main(["transform", "-a", "rotate-45,rotate-90", "--limit", "10"])
    main(["transform", "-r", "self,rotate-45,rotate-90", "--limit", "3"])
    ret = main(["commit", "-m", "create a new dataset"])

    collection_file_paths = list(
        (ldb_instance / InstanceDir.COLLECTIONS).glob("*/*"),
    )
    transform_mapping_file_paths = list(
        (ldb_instance / InstanceDir.TRANSFORM_MAPPINGS).glob("*/*"),
    )
    dataset_version_file_paths = list(
        (ldb_instance / InstanceDir.DATASET_VERSIONS).glob("*/*"),
    )
    dataset_file_paths = list((ldb_instance / InstanceDir.DATASETS).glob("*"))

    collection_obj = load_data_file(collection_file_paths[0])
    transform_mapping_obj = load_data_file(transform_mapping_file_paths[0])
    dataset_version_obj = DatasetVersion.parse(
        load_data_file(dataset_version_file_paths[0]),
    )
    dataset_obj = Dataset.parse(load_data_file(dataset_file_paths[0]))
    collection_hash = (
        collection_file_paths[0].parent.name + collection_file_paths[0].name
    )
    transform_mapping_hash = (
        transform_mapping_file_paths[0].parent.name
        + transform_mapping_file_paths[0].name
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
        parent="",
        collection=collection_hash,
        transform_mapping_id=transform_mapping_hash,
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
    assert len(transform_mapping_file_paths) == 1
    assert len(dataset_version_file_paths) == 1
    assert len(dataset_file_paths) == 1
    assert len(collection_obj) == 32
    assert len(transform_mapping_obj) == 10
    assert sum(bool(a) for a in collection_obj.values()) == 23
    assert dataset_obj == expected_dataset_obj
    assert dataset_version_obj == expected_dataset_version_obj


def test_commit_multiple_versions(data_dir, ldb_instance, workspace_path):
    paths = [
        data_dir / "fashion-mnist/original/has_both/test",
        data_dir / "fashion-mnist/original/has_both/train/000[01]*",
        data_dir / "fashion-mnist/updates",
    ]
    for path_obj in paths:
        path = os.fspath(path_obj)
        main(["index", "-m", "bare", path])
        main(["add", path])
        main(
            [
                "commit",
                "-m",
                f"add {os.fspath(path_obj.relative_to(data_dir))}",
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
        path_list.sort(key=lambda p: p.stat().st_mtime or 0)

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
        "add " + os.fspath(Path("fashion-mnist/original/has_both/test")),
        "add "
        + os.fspath(Path("fashion-mnist/original/has_both/train/000[01]*")),
        "add " + os.fspath(Path("fashion-mnist/updates")),
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


def test_commit_empty_workspace_dataset(
    data_dir,
    ldb_instance,
    workspace_path,
):
    ret = main(["commit", "-m", "create a new dataset"])
    assert ret == 0
    assert not list((ldb_instance / InstanceDir.DATASETS).iterdir())


def test_commit_no_changes(data_dir, ldb_instance, workspace_path):
    dir_to_add = os.fspath(data_dir / "fashion-mnist/original")
    main(["index", "-m", "bare", dir_to_add])
    main(["add", dir_to_add])
    main(["commit", "-m", "create a new dataset"])
    ret = main(["commit", "-m", "create another version"])
    assert ret == 0
    assert len(list((ldb_instance / InstanceDir.DATASETS).iterdir())) == 1


def test_commit_without_workspace_dataset(tmp_path, data_dir, ldb_instance):
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    os.chdir(workspace_path)
    ret = main(["commit", "-m", "create a new dataset"])
    assert ret == 1
    assert not list((ldb_instance / InstanceDir.DATASETS).iterdir())

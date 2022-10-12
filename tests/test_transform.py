from ldb.core import LDBClient
from ldb.main import main
from ldb.path import WorkspacePath
from ldb.transform import (
    SELF,
    UpdateType,
    add_transforms_with_data_objects,
    get_transform_infos_from_dir,
)


def test_transform_add(
    ldb_instance,
    workspace_path,
    staged_ds_a,
    transform_infos,
):
    client = LDBClient(ldb_instance)
    r45 = transform_infos["rotate-45"]
    r90 = transform_infos["rotate-90"]
    add_transforms_with_data_objects(
        workspace_path,
        client,
        ["3c679fd1b8537dc7da1272a085e388e6"],
        [SELF.transform],
        update_type=UpdateType.DEL,
    )
    ret = main(
        [
            "transform",
            "id:3c679fd1b8537dc7da1272a085e388e6",
            "id:982814b9116dce7882dfc31636c3ff7a",
            "id:ebbc6c0cebb66738942ee56513f9ee2f",
            "-a",
            "rotate-45,rotate-90",
        ],
    )
    result = get_transform_infos_from_dir(
        client,
        workspace_path / WorkspacePath.TRANSFORM_MAPPING,
    )
    expected = {
        "3c679fd1b8537dc7da1272a085e388e6": frozenset({r45, r90}),
        "982814b9116dce7882dfc31636c3ff7a": frozenset({SELF, r45, r90}),
        "ebbc6c0cebb66738942ee56513f9ee2f": frozenset({SELF, r45, r90}),
    }
    assert ret == 0
    assert result == expected


def test_transform_delete(
    ldb_instance,
    workspace_path,
    staged_ds_a,
    transform_infos,
):
    client = LDBClient(ldb_instance)
    r45 = transform_infos["rotate-45"]
    r90 = transform_infos["rotate-90"]
    add_transforms_with_data_objects(
        workspace_path,
        client,
        [
            "3c679fd1b8537dc7da1272a085e388e6",
            "982814b9116dce7882dfc31636c3ff7a",
            "ebbc6c0cebb66738942ee56513f9ee2f",
        ],
        [r45.transform, r90.transform],
        update_type=UpdateType.ADD,
    )
    ret = main(
        [
            "transform",
            "id:3c679fd1b8537dc7da1272a085e388e6",
            "id:ebbc6c0cebb66738942ee56513f9ee2f",
            "-r",
            "rotate-45,rotate-90",
        ],
    )
    result = get_transform_infos_from_dir(
        client,
        workspace_path / WorkspacePath.TRANSFORM_MAPPING,
    )
    r90 = transform_infos["rotate-90"]
    r45 = transform_infos["rotate-45"]
    expected = {
        "982814b9116dce7882dfc31636c3ff7a": frozenset({SELF, r45, r90}),
    }
    assert ret == 0
    assert result == expected


def test_transform_set(
    ldb_instance,
    workspace_path,
    staged_ds_a,
    transform_infos,
):
    client = LDBClient(ldb_instance)
    r45 = transform_infos["rotate-45"]
    r90 = transform_infos["rotate-90"]
    add_transforms_with_data_objects(
        workspace_path,
        client,
        [
            "3c679fd1b8537dc7da1272a085e388e6",
            "982814b9116dce7882dfc31636c3ff7a",
        ],
        [SELF.transform],
        update_type=UpdateType.DEL,
    )
    add_transforms_with_data_objects(
        workspace_path,
        client,
        [
            "3c679fd1b8537dc7da1272a085e388e6",
            "ebbc6c0cebb66738942ee56513f9ee2f",
        ],
        [r45.transform, r90.transform],
        update_type=UpdateType.ADD,
    )
    ret = main(
        [
            "transform",
            "id:3c679fd1b8537dc7da1272a085e388e6",
            "id:982814b9116dce7882dfc31636c3ff7a",
            "id:ebbc6c0cebb66738942ee56513f9ee2f",
            "-s",
            "rotate-45,rotate-90",
        ],
    )
    result = get_transform_infos_from_dir(
        client,
        workspace_path / WorkspacePath.TRANSFORM_MAPPING,
    )
    expected = {
        "3c679fd1b8537dc7da1272a085e388e6": frozenset({r45, r90}),
        "982814b9116dce7882dfc31636c3ff7a": frozenset({r45, r90}),
        "ebbc6c0cebb66738942ee56513f9ee2f": frozenset({r45, r90}),
    }
    assert ret == 0
    assert result == expected

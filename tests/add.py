import os
import shutil

import pytest

from ldb import config
from ldb.core import add_default_read_add_storage
from ldb.main import main
from ldb.path import Filename
from ldb.utils import DATASET_PREFIX, ROOT, WORKSPACE_DATASET_PREFIX

from .data import QUERY_DATA
from .utils import (
    DATA_DIR,
    get_staged_object_file_paths,
    get_workspace_counts,
    num_empty_files,
    stage_new_workspace,
)


class AddCommandBase:
    """
    A class for tests that are reusable for the sync command.

    These tests can be used for the sync command through class
    inheritance. Tests only used for the add command should be top-level
    functions.
    """

    COMMAND = ""
    PHYSICAL = False

    @pytest.mark.parametrize(
        "args,data_objs,annots",
        QUERY_DATA.values(),
        ids=QUERY_DATA.keys(),
    )
    def test_add_root_dataset(
        self,
        args,
        data_objs,
        annots,
        fashion_mnist_session,
        global_workspace_path,
    ):
        cmd_args = [self.COMMAND, f"{DATASET_PREFIX}{ROOT}", *args]
        if self.PHYSICAL:
            cmd_args.append("--physical")
        ret = main(cmd_args)
        object_file_paths = get_staged_object_file_paths(global_workspace_path)
        assert ret == 0
        assert len(object_file_paths) == data_objs
        assert num_empty_files(object_file_paths) == annots
        if self.PHYSICAL:
            assert get_workspace_counts(global_workspace_path) == (data_objs, annots)
        else:
            assert get_workspace_counts(global_workspace_path) == (0, 0)

    @pytest.mark.parametrize(
        "index_first,objects,annots",
        [
            (True, 32, 23),
            (False, 23, 23),
        ],
    )
    def test_add_path(self, index_first, objects, annots, workspace_path):
        dir_to_add = os.fspath(DATA_DIR / "fashion-mnist/original")
        if index_first:
            ret = main(["index", "-m", "bare", dir_to_add])
        cmd_args = [self.COMMAND, dir_to_add]
        if self.PHYSICAL:
            cmd_args.append("--physical")
        ret = main(cmd_args)
        object_file_paths = get_staged_object_file_paths(workspace_path)
        assert ret == 0
        assert len(object_file_paths) == objects
        assert num_empty_files(object_file_paths) == annots
        if self.PHYSICAL:
            assert get_workspace_counts(workspace_path) == (objects, annots)
        else:
            assert get_workspace_counts(workspace_path) == (0, 0)

    def test_add_path_without_auto_index(
        self,
        ldb_instance,
        workspace_path,
    ):
        dir_to_add = os.fspath(DATA_DIR / "fashion-mnist/original")
        with config.edit(ldb_instance / Filename.CONFIG) as cfg:
            if "core" not in cfg:
                cfg["core"] = {}
            cfg["core"]["auto_index"] = False  # type: ignore[index]
        cmd_args = [self.COMMAND, dir_to_add]
        if self.PHYSICAL:
            cmd_args.append("--physical")
        ret = main(cmd_args)
        assert ret == 1

    def test_add_data_objects(self, workspace_path, index_original):
        cmd_args = [
            self.COMMAND,
            "id:3c679fd1b8537dc7da1272a085e388e6",
            "id:982814b9116dce7882dfc31636c3ff7a",
            "id:ebbc6c0cebb66738942ee56513f9ee2f",
            "id:1e0759182b328fd22fcdb5e6beb54adf",
        ]
        if self.PHYSICAL:
            cmd_args.append("--physical")
        ret = main(cmd_args)
        object_file_paths = get_staged_object_file_paths(workspace_path)
        assert ret == 0
        assert len(object_file_paths) == 4
        assert num_empty_files(object_file_paths) == 2
        if self.PHYSICAL:
            assert get_workspace_counts(workspace_path) == (4, 2)
        else:
            assert get_workspace_counts(workspace_path) == (0, 0)

    def test_add_datasets(self, workspace_path, ds_a, ds_b):
        main(["stage", f"{DATASET_PREFIX}c"])
        cmd_args = [self.COMMAND, ds_a, ds_b]
        if self.PHYSICAL:
            cmd_args.append("--physical")
        ret = main(cmd_args)
        object_file_paths = get_staged_object_file_paths(workspace_path)
        assert ret == 0
        assert len(object_file_paths) == 7
        assert num_empty_files(object_file_paths) == 3
        if self.PHYSICAL:
            assert get_workspace_counts(workspace_path) == (7, 3)
        else:
            assert get_workspace_counts(workspace_path) == (0, 0)

    def test_add_root_dataset_query(self, workspace_path, index_original):
        cmd_args = [
            self.COMMAND,
            f"{DATASET_PREFIX}{ROOT}",
            "--query",
            "label != `null` && label > `2` && label < `8`",
        ]
        if self.PHYSICAL:
            cmd_args.append("--physical")
        ret = main(cmd_args)
        object_file_paths = get_staged_object_file_paths(workspace_path)
        assert ret == 0
        assert len(object_file_paths) == 14
        assert num_empty_files(object_file_paths) == 14
        if self.PHYSICAL:
            assert get_workspace_counts(workspace_path) == (14, 14)
        else:
            assert get_workspace_counts(workspace_path) == (0, 0)

    @pytest.mark.parametrize(
        "index_first,objects,annots",
        [
            (True, 32, 23),
            (False, 23, 23),
        ],
    )
    def test_add_current_workspace(
        self,
        index_first,
        objects,
        annots,
        workspace_path,
        ldb_instance,
    ):
        add_default_read_add_storage(ldb_instance)
        shutil.copytree(
            DATA_DIR / "fashion-mnist/original",
            "./train",
        )
        if index_first:
            main(["index", "-m", "bare", "."])
        cmd_args = [self.COMMAND, "."]
        if self.PHYSICAL:
            cmd_args.append("--physical")
        ret = main(cmd_args)
        object_file_paths = get_staged_object_file_paths(workspace_path)
        assert ret == 0
        assert len(object_file_paths) == objects
        assert num_empty_files(object_file_paths) == annots
        original_objects = 33
        original_annots = 31
        if self.PHYSICAL:
            assert get_workspace_counts(workspace_path) == (
                original_objects + objects,
                original_annots + annots,
            )
        else:
            assert get_workspace_counts(workspace_path) == (original_objects, original_annots)

    @pytest.mark.parametrize(
        "index_first,objects,annots",
        [
            (True, 32, 23),
            (False, 23, 23),
        ],
    )
    def test_add_another_workspace(
        self,
        index_first,
        objects,
        annots,
        workspace_path,
        data_dir,
        ldb_instance,
        tmp_path,
    ):
        dir_to_add = os.fspath(data_dir / "fashion-mnist/original")
        other_workspace_path = tmp_path / "other-workspace"
        stage_new_workspace(other_workspace_path)
        os.chdir(other_workspace_path)
        if index_first:
            main(["index", "-m", "bare", dir_to_add])
        main(["add", dir_to_add])
        os.chdir(workspace_path)
        cmd_args = [
            self.COMMAND,
            f"{WORKSPACE_DATASET_PREFIX}{os.fspath(other_workspace_path)}",
        ]
        if self.PHYSICAL:
            cmd_args.append("--physical")
        ret = main(cmd_args)

        object_file_paths = get_staged_object_file_paths(workspace_path)
        assert ret == 0
        assert len(object_file_paths) == objects
        assert num_empty_files(object_file_paths) == annots
        if self.PHYSICAL:
            assert get_workspace_counts(workspace_path) == (objects, annots)
        else:
            assert get_workspace_counts(workspace_path) == (0, 0)

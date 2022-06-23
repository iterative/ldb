import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    Collection,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from ldb.dataset import (
    get_annotation,
    get_collection_dir_items,
    get_data_object_meta,
)
from ldb.main import main
from ldb.path import InstanceDir, WorkspacePath
from ldb.stage import stage_workspace
from ldb.typing import JSONDecoded
from ldb.utils import chmod_minus_x, current_time, json_dumps, load_data_file
from ldb.workspace import WorkspaceDataset, iter_workspace_dir

DATA_DIR = Path(__file__).parent.parent / "data"
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
DATA_OBJECT_KEYS = (
    "alternate_paths",
    "first_indexed",
    "fs",
    "last_indexed",
    "last_indexed_by",
    "tags",
    "type",
)
DATA_OBJECT_FS_KEYS = (
    "atime",
    "ctime",
    "fs_id",
    "gid",
    "mode",
    "mtime",
    "path",
    "protocol",
    "size",
    "uid",
)
ANNOTATION_META_KEYS = (
    "first_indexed_time",
    "last_indexed_time",
    "mtime",
    "version",
)
ANNOTATION_LDB_KEYS = (
    "schema_version",
    "user_version",
)
DEFAULT_TAG_SEQS = (
    ("a", "b", "c"),
    ("b", "d"),
)


def is_data_object_meta_obj(data: Dict[str, Any]) -> bool:
    return (
        tuple(data.keys()) == DATA_OBJECT_KEYS
        and tuple(data["fs"].keys()) == DATA_OBJECT_FS_KEYS
    )


def is_data_object_meta(file_path: Path) -> bool:
    return is_data_object_meta_obj(load_data_file(file_path))


def is_annotation_meta(file_path: Path) -> bool:
    return (
        tuple(load_data_file(file_path).keys()) == ANNOTATION_META_KEYS
        and (file_path.parent.parent / "current").is_file()
    )


def is_annotation(dir_path: Path) -> bool:
    return tuple(
        load_data_file(dir_path / "ldb"),
    ) == ANNOTATION_LDB_KEYS and bool(load_data_file(dir_path / "user"))


def get_data_object_meta_file_paths(ldb_instance: Path) -> List[Path]:
    return list((ldb_instance / InstanceDir.DATA_OBJECT_INFO).glob("*/*/meta"))


def get_annotation_meta_file_paths(ldb_instance: Path) -> List[Path]:
    return list(
        (ldb_instance / InstanceDir.DATA_OBJECT_INFO).glob(
            "*/*/annotations/*",
        ),
    )


def get_annotation_dir_paths(ldb_instance: Path) -> List[Path]:
    return list((ldb_instance / InstanceDir.ANNOTATIONS).glob("*/*"))


def get_indexed_data_paths(
    ldb_dir: Path,
) -> Tuple[List[Path], List[Path], List[Path]]:
    return (
        get_data_object_meta_file_paths(ldb_dir),
        get_annotation_meta_file_paths(ldb_dir),
        get_annotation_dir_paths(ldb_dir),
    )


def get_obj_tags(paths: Sequence[Path]) -> List[List[str]]:
    return [load_data_file(p)["tags"] for p in paths]


def stage_new_workspace(
    path: Path,
    name: str = "my-dataset",
    staged_time: Optional[datetime] = None,
    parent: str = "",
    tags: Optional[List[str]] = None,
) -> None:
    if staged_time is None:
        staged_time = current_time()
    if tags is None:
        tags = []
    stage_workspace(
        path,
        WorkspaceDataset(
            dataset_name=name,
            staged_time=staged_time,
            parent=parent,
            tags=tags,
        ),
    )


def get_staged_object_file_paths(workspace_path: Path) -> List[Path]:
    return list((workspace_path / WorkspacePath.COLLECTION).glob("*/*"))


def num_empty_files(paths: Iterable[Path]) -> int:
    num = 0
    for path in paths:
        num += bool(path.read_text())
    return num


def add_user_filter(ldb_dir: Path) -> None:
    dest = ldb_dir / InstanceDir.USER_FILTERS
    if os.name == "nt":
        py_dest = dest / "reverse.py"
        shutil.copy2(SCRIPTS_DIR / "reverse", py_dest)
        chmod_minus_x(py_dest)
        shutil.copy2(SCRIPTS_DIR / "reverse.bat", dest)
    else:
        shutil.copy2(SCRIPTS_DIR / "reverse", dest)


def index_fashion_mnist(  # pylint: disable=unused-argument
    ldb_instance: Path,
    tags_seqs: Tuple[Collection[str], Collection[str]] = DEFAULT_TAG_SEQS,
) -> None:
    paths = (
        DATA_DIR / "fashion-mnist/original",
        DATA_DIR / "fashion-mnist/updates",
    )
    for path_obj, tags in zip(paths, tags_seqs):
        tag_args = ["--add-tags", ",".join(tags)] if tags else []
        main(["index", "-m", "bare", *tag_args, os.fspath(path_obj)])


def get_workspace_counts(workspace_path: Union[str, Path]) -> Tuple[int, int]:
    all_files: List[str] = []
    for entry in iter_workspace_dir(workspace_path):
        if entry.is_file():
            all_files.append(entry.name)
        else:
            for _, _, files in os.walk(entry.path):
                all_files.extend(files)
    num_annotations = sum(f.endswith(".json") for f in all_files)
    num_data_objects = len(all_files) - num_annotations
    return num_data_objects, num_annotations


def create_data_lake(
    base_dir: Union[str, Path],
    data: Mapping[str, JSONDecoded],
    make_data_obj: bool = True,
    make_annot: bool = True,
) -> None:
    base_dir = Path(base_dir)
    base_dir.mkdir(parents=True)
    for base_name, annot_contents in data.items():
        (base_dir / base_name).parent.mkdir(parents=True, exist_ok=True)

        if make_data_obj:
            data_obj_name = f"{base_name}.png"
            data_obj_dest = base_dir / data_obj_name
            data_obj_dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(
                DATA_DIR / "flat-data-object-only" / data_obj_name,
                data_obj_dest,
            )

        if make_annot:
            annot_dest = base_dir / f"{base_name}.json"
            annot_contents_str = json_dumps(annot_contents)
            with open(annot_dest, "x", encoding="utf-8") as annot_file:
                annot_file.write(annot_contents_str)


def get_current_annot_info(ldb_dir: Path) -> Dict[str, JSONDecoded]:
    result = {}
    for data_obj_hash, annot_hash in get_collection_dir_items(
        ldb_dir / InstanceDir.DATA_OBJECT_INFO,
        is_workspace=False,
    ):
        name = os.path.splitext(
            os.path.basename(
                get_data_object_meta(ldb_dir, data_obj_hash)["fs"]["path"],
            ),
        )[0]
        annot = get_annotation(ldb_dir, annot_hash or "")
        result[name] = annot
    return result

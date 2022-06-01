import re
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Dict,
    FrozenSet,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from ldb.add import ArgType, get_arg_type, process_args_for_ls
from ldb.dataset import OpDef, apply_queries
from ldb.path import InstanceDir, WorkspacePath
from ldb.string_utils import left_truncate
from ldb.transform import (
    DEFAULT,
    TransformInfo,
    TransformType,
    get_transform_infos_from_dir,
)
from ldb.utils import DATA_OBJ_ID_PREFIX, get_hash_path, load_data_file


@dataclass
class DatasetListing:
    data_object_hash: str
    data_object_path: str
    annotation_hash: str
    annotation_version: int
    transform_info: FrozenSet[TransformInfo] = DEFAULT


def ls(
    ldb_dir: Path,
    paths: Sequence[str],
    collection_ops: Iterable[OpDef],
    warn: bool = True,
) -> List[DatasetListing]:
    transform_infos = get_workspace_transform_infos(ldb_dir, paths)
    data_object_hashes, annotation_hashes, _ = process_args_for_ls(
        ldb_dir,
        paths,
    )
    collection = apply_queries(
        ldb_dir,
        data_object_hashes,
        annotation_hashes,
        collection_ops,
        warn=warn,
    )
    return ls_collection(ldb_dir, collection, transform_infos=transform_infos)


def get_workspace_transform_infos(
    ldb_dir: Path,
    paths: Sequence[str],
) -> Optional[Dict[str, FrozenSet[TransformInfo]]]:
    # TODO: avoid resolving paths here and also inside process_args_for_ls
    ws_path = ""
    if not paths or (get_arg_type(paths) == ArgType.WORKSPACE_DATASET):
        paths = [re.sub(r"^ws:", "", p) for p in paths]
        if not paths:
            ws_path = "."
        elif len(paths) == 1:
            ws_path = paths[0]

    if ws_path:
        return get_transform_infos_from_dir(
            ldb_dir,
            Path(ws_path) / WorkspacePath.TRANSFORM_MAPPING,
        )
    return None


def transform_info_sort_key(info: TransformInfo) -> Tuple[bool, str]:
    return (
        info.transform.transform_type != TransformType.PREDEFINED,
        info.display_name,
    )


def print_dataset_listings(
    dataset_listings: List[DatasetListing],
    verbose: bool = False,
) -> int:
    if not dataset_listings:
        return 0

    num_items = 0
    print(
        f"{'Data Object Hash':36}  "
        f"{'Annot':3}  "
        f"{'Data Object Path':24}  "
        f"{'Transforms':24}",
    )
    for item in dataset_listings:
        annotation_version = str(item.annotation_version or "-")
        path = (
            item.data_object_path
            if verbose
            else left_truncate(item.data_object_path, 24)
        )
        transform_infos = sorted(
            item.transform_info,
            key=transform_info_sort_key,
        )
        transforms = ",".join(t.display_name for t in transform_infos) or "-"
        data_obj = f"{DATA_OBJ_ID_PREFIX}{item.data_object_hash}"
        print(
            f" {data_obj:36}  "
            f"{annotation_version:4}   {path:24}  {transforms}",
        )
        num_items += 1
    return num_items


def ls_collection(
    ldb_dir: Path,
    collection: Iterable[Tuple[str, Optional[str]]],
    transform_infos: Optional[Mapping[str, FrozenSet[TransformInfo]]] = None,
) -> List[DatasetListing]:
    result = []
    data_object_info_path = ldb_dir / InstanceDir.DATA_OBJECT_INFO
    for data_object_hash, annotation_hash in collection:
        data_object_dir = get_hash_path(
            data_object_info_path,
            data_object_hash,
        )
        annotation_version = 0
        if annotation_hash:
            annotation_meta = load_data_file(
                data_object_dir / "annotations" / annotation_hash,
            )
            annotation_version = annotation_meta["version"]
        data_object_meta = load_data_file(data_object_dir / "meta")

        if transform_infos is None:
            transform_info: FrozenSet[TransformInfo] = DEFAULT
        else:
            transform_info = transform_infos.get(data_object_hash, DEFAULT)
        result.append(
            DatasetListing(
                data_object_hash=data_object_hash,
                data_object_path=data_object_meta["fs"]["path"],
                annotation_hash=annotation_hash or "",
                annotation_version=annotation_version,
                transform_info=transform_info,
            ),
        )
    return result

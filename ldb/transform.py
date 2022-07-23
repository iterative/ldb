import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    FrozenSet,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from funcy.objects import cached_property

from ldb import config
from ldb.config import load_first
from ldb.core import get_ldb_instance
from ldb.dataset import (
    DatasetVersion,
    OpDef,
    get_dataset,
    get_dataset_version_hash,
    iter_collection_dir,
)
from ldb.exceptions import LDBException
from ldb.path import Filename, InstanceDir, WorkspacePath
from ldb.utils import (
    ROOT,
    StrEnum,
    format_dataset_identifier,
    get_hash_path,
    hash_data,
    json_dumps,
    load_data_file,
    parse_dataset_identifier,
    write_data_file,
)
from ldb.workspace import load_workspace_dataset

MergeFunc = Callable[[Set[str], Set[str]], Set[str]]


class UpdateType(StrEnum):
    ADD = "add"
    DEL = "del"
    SET = "set"


class TransformType:
    EXEC = "exec"
    PREDEFINED = "predefined"


@dataclass
class TransformConfig:
    name: str
    run: Sequence[str]
    create_annotations: bool = True

    @classmethod
    def all(cls, ldb_dir: Path) -> List["TransformConfig"]:
        cfg = load_first(ldb_dir=ldb_dir)
        result = []
        if cfg is not None:
            builtin_names = {t.name for t in BUILTIN}
            for name, values in cfg.get("transform", {}).items():
                if name in builtin_names:
                    raise ValueError(
                        "Builtin transform name {name!r} is reserved and may "
                        "not be configured",
                    )
                result.append(cls(name=name, **values))
        return result

    def to_dict(self) -> Dict[str, Dict[str, Any]]:
        flat_dict = asdict(self)
        if flat_dict["create_annotations"]:
            flat_dict.pop("create_annotations")
        return {flat_dict.pop("name"): flat_dict}

    def save(self, ldb_dir: Path) -> None:
        to_write = self.to_dict()
        with config.edit(ldb_dir / Filename.CONFIG) as cfg:
            transform_cfg = cfg.setdefault("transform", {})
            transform_cfg.update(to_write)


@dataclass(frozen=True)
class TransformInfo:
    transform: "Transform"
    name: str = ""
    create_annotations: bool = True

    @property
    def display_name(self) -> str:
        return self.name or self.transform.obj_id

    @classmethod
    def from_generic(
        cls,
        transform_value: Union[str, Iterable[str]] = "",
        name: str = "",
        create_annotations: bool = True,
        transform_type: str = TransformType.EXEC,
    ) -> "TransformInfo":
        return cls(
            Transform.from_generic(transform_value, transform_type),
            name=name,
            create_annotations=create_annotations,
        )

    @classmethod
    def all(cls, ldb_dir: Path) -> List["TransformInfo"]:
        transforms = Transform.all(ldb_dir)
        transform_infos = {
            t.transform.obj_id: t
            for t in TransformInfo.all_configured(ldb_dir)
        }
        result = []
        for transform in transforms:
            info = transform_infos.get(transform.obj_id)
            if info is None:
                info = TransformInfo(transform=transform)
            result.append(info)
        return result

    @classmethod
    def all_configured(cls, ldb_dir: Path) -> List["TransformInfo"]:
        result = []
        for transform_config in TransformConfig.all(ldb_dir):
            result.append(
                TransformInfo(
                    transform=Transform.from_generic(transform_config.run),
                    name=transform_config.name,
                    create_annotations=transform_config.create_annotations,
                ),
            )
        result.extend(BUILTIN)
        return result

    def save(self, ldb_dir: Path) -> None:
        self.transform.save(ldb_dir)
        if self.name and self.transform.transform_type == TransformType.EXEC:
            if self.name in BUILTIN_NAMES:
                raise ValueError(
                    "Cannot configure transform with reserved name: "
                    f"{self.name!r}",
                )
            if not isinstance(self.transform.value, Sequence):
                raise ValueError(
                    f"value for transforms of type {TransformType.EXEC} "
                    f"must be a sequence, got {type(self.transform.value)}",
                )
            TransformConfig(
                name=self.name,
                run=self.transform.value,
                create_annotations=self.create_annotations,
            ).save(ldb_dir)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "transform": self.transform.to_dict(),
            "name": self.name,
            "create_annotations": self.create_annotations,
        }


@dataclass(frozen=True)
class Transform:
    value: Union[str, Tuple[str, ...]] = ""
    transform_type: str = TransformType.EXEC

    @classmethod
    def from_generic(
        cls,
        value: Union[str, Iterable[str]] = "",
        transform_type: str = TransformType.EXEC,
    ) -> "Transform":
        if not isinstance(value, (str, tuple)):
            value = tuple(value)
        return cls(value, transform_type)

    @classmethod
    def load(cls, ldb_dir: Path, obj_id: str) -> "Transform":
        obj_dir = ldb_dir / InstanceDir.TRANSFORMS
        return cls.from_generic(
            **load_data_file(get_hash_path(obj_dir, obj_id)),
        )

    @classmethod
    def _save_builtins(cls, ldb_dir: Path) -> None:
        for transform_info in BUILTIN:
            transform_info.transform.save(ldb_dir)

    @classmethod
    def all(cls, ldb_dir: Path) -> List["Transform"]:
        cls._save_builtins(ldb_dir)
        result = []
        for f in iter_collection_dir(ldb_dir / InstanceDir.TRANSFORMS):
            result.append(cls(**load_data_file(Path(f))))
        return result

    def to_dict(self) -> Dict[str, Union[str, Tuple[str, ...]]]:
        return {
            "transform_type": self.transform_type,
            "value": self.value,
        }

    @cached_property
    def json(self) -> str:
        return json_dumps(self.to_dict())

    @cached_property
    def obj_id(self) -> str:
        return hash_data(self.json.encode())

    def save(self, ldb_dir: Path) -> None:
        path = get_hash_path(
            ldb_dir / InstanceDir.TRANSFORMS,
            self.obj_id,
        )
        write_data_file(path, self.json.encode(), overwrite_existing=False)


def builtin(
    name: str,
    create_annotations: bool = True,
) -> TransformInfo:
    return TransformInfo(
        transform=Transform(
            value=name,
            transform_type=TransformType.PREDEFINED,
        ),
        name=name,
        create_annotations=create_annotations,
    )


SELF = builtin("self")
BUILTIN = frozenset({SELF})
BUILTIN_NAMES = frozenset({t.name for t in BUILTIN})
DEFAULT = frozenset({SELF})


def add_transform(
    workspace_path: Path,
    paths: Sequence[str],
    query_args: Iterable[OpDef],
    transform_names: Iterable[str],
    update_type: UpdateType = UpdateType.ADD,
    ldb_dir: Optional[Path] = None,
) -> None:
    from ldb.add import (  # pylint: disable=import-outside-toplevel
        select_data_object_hashes,
    )

    if ldb_dir is None:
        ldb_dir = get_ldb_instance()
    transforms = [
        t.transform for t in get_transforms_by_name(ldb_dir, transform_names)
    ]
    if not transforms:
        raise ValueError("Transform name list is empty")

    workspace_path = Path(os.path.normpath(workspace_path))
    ds_name = load_workspace_dataset(workspace_path).dataset_name
    ds_ident = format_dataset_identifier(ds_name)

    data_object_hashes = select_data_object_hashes(
        get_ldb_instance(),
        paths,
        query_args,
        warn=False,
    )
    num_deleted = add_transforms_with_data_objects(
        workspace_path,
        ldb_dir,
        data_object_hashes,
        transforms=transforms,
        update_type=update_type,
    )
    update_verb = "Updated"
    if update_type == UpdateType.ADD:
        update_verb = "Added"
    elif update_type == UpdateType.DEL:
        update_verb = "Removed"
    elif update_type == UpdateType.SET:
        update_verb = "Set"
    print(
        f"{update_verb} transforms for {num_deleted} data objects in "
        f"{ds_ident}",
    )


def get_transforms_by_name(
    ldb_dir: Path,
    names: Iterable[str],
) -> List[TransformInfo]:
    transform_mapping = {
        t.name: t for t in TransformInfo.all_configured(ldb_dir)
    }
    transforms_by_id = {t.obj_id: t for t in Transform.all(ldb_dir)}
    transforms = []
    for name in names:
        try:
            transform_info = transform_mapping[name]
        except KeyError as exc:
            transform = transforms_by_id.get(name)
            if transform is None:
                raise LDBException(
                    f"No transform found with the name {name}",
                ) from exc
            transform_info = TransformInfo(transform=transform)
        transforms.append(transform_info)
    return transforms


def add_transforms_with_data_objects(
    workspace_path: Path,
    ldb_dir: Path,
    data_object_hashes: Iterable[str],
    transforms: Iterable[Transform] = (),
    update_type: UpdateType = UpdateType.ADD,
) -> int:
    transform_mapping_dir_path = (
        workspace_path / WorkspacePath.TRANSFORM_MAPPING
    )
    transform_mapping_dir_path.mkdir(exist_ok=True)
    transform_obj_ids = []
    for t in transforms:
        t.save(ldb_dir)
        transform_obj_ids.append(t.obj_id)
    return update_transform_mapping_dir(
        transform_mapping_dir_path,
        transform_obj_ids,
        data_object_hashes,
        update_type=update_type,
    )


def merge_func_for_set(_s1: Set[str], s2: Set[str]) -> Set[str]:
    return s2


def update_transform_mapping_dir(
    collection_dir_path: Path,
    transform_hashes: Iterable[str],
    data_object_hashes: Iterable[str],
    update_type: UpdateType = UpdateType.ADD,
) -> int:
    default_id_list = sorted(t.transform.obj_id for t in DEFAULT)

    transform_hash_set = set(transform_hashes)
    if update_type == UpdateType.ADD:
        merge_func: MergeFunc = set.union
    elif update_type == UpdateType.DEL:
        merge_func = set.difference
    elif update_type == UpdateType.SET:
        merge_func = merge_func_for_set
    else:
        raise ValueError(f"invalid update_type: {update_type}")
    starting_ids = sorted(merge_func(set(default_id_list), transform_hash_set))
    starting_ids_are_default = starting_ids == default_id_list

    default_str = ""
    if not starting_ids_are_default:
        default_str = json_dumps(starting_ids)

    num_data_objects = 0
    for data_object_hash in data_object_hashes:
        collection_member_path = get_hash_path(
            collection_dir_path,
            data_object_hash,
        )
        if collection_member_path.exists():
            delete = False
            with collection_member_path.open("r+") as file:
                existing_annotation_hash = json.loads(file.read())
                new = sorted(
                    merge_func(
                        set(existing_annotation_hash),
                        transform_hash_set,
                    ),
                )
                if new == default_id_list:
                    delete = True
                    num_data_objects += 1
                elif new != existing_annotation_hash:
                    new_str = json_dumps(new)
                    file.seek(0)
                    file.write(new_str)
                    file.truncate()
                    num_data_objects += 1
            if delete:
                collection_member_path.unlink()
        elif not starting_ids_are_default:
            collection_member_path.parent.mkdir(exist_ok=True)
            with collection_member_path.open("w") as file:
                file.write(default_str)
                num_data_objects += 1
    return num_data_objects


def get_transform_mapping_dir_items(
    transform_mapping_dir: Path,
) -> Iterator[Tuple[str, List[str]]]:
    for path in sorted(transform_mapping_dir.glob("*/*")):
        yield path.parent.name + path.name, json.loads(path.read_text())


def get_transform_infos_by_hash(
    ldb_dir: Path,
    hashes: Iterable[str],
) -> Dict[str, TransformInfo]:
    infos = {}
    for transform_info in TransformInfo.all_configured(ldb_dir):
        infos[transform_info.transform.obj_id] = transform_info

    result = {}
    for h in hashes:
        info = infos.get(h)
        if info is None:
            info = TransformInfo(transform=Transform.load(ldb_dir, h))
        result[h] = info
    return result


def get_transform_infos_from_dir(
    ldb_dir: Path,
    transform_mapping_dir: Path,
) -> Dict[str, FrozenSet[TransformInfo]]:
    return get_transform_infos_from_items(
        ldb_dir,
        get_transform_mapping_dir_items(transform_mapping_dir),
    )


def dataset_identifier_to_transform_ids(
    ldb_dir: Path,
    dataset_identifier: str,
) -> Dict[str, List[str]]:
    dataset_name, dataset_version = parse_dataset_identifier(
        dataset_identifier,
    )
    if dataset_name == ROOT:
        return {}
    dataset = get_dataset(ldb_dir, dataset_name)
    dataset_version_hash = get_dataset_version_hash(
        dataset,
        dataset_version,
    )
    dataset_version_obj = DatasetVersion.parse(
        load_data_file(
            get_hash_path(
                ldb_dir / InstanceDir.DATASET_VERSIONS,
                dataset_version_hash,
            ),
        ),
    )
    return load_data_file(  # type: ignore[no-any-return]
        get_hash_path(
            ldb_dir / InstanceDir.TRANSFORM_MAPPINGS,
            dataset_version_obj.transform_mapping_id,
        ),
    )


def get_transform_infos_from_items(
    ldb_dir: Path,
    transform_info_items: Iterable[Tuple[str, Collection[str]]],
) -> Dict[str, FrozenSet[TransformInfo]]:
    transform_info_items = list(transform_info_items)
    unique_hashes = {
        h for _, hash_seq in transform_info_items for h in hash_seq
    }
    transform_infos = get_transform_infos_by_hash(ldb_dir, unique_hashes)
    return {
        d: frozenset({transform_infos[h] for h in hash_seq})
        for d, hash_seq in transform_info_items
    }


def transform_dir_to_object(transform_dir: Path) -> Dict[str, List[str]]:
    return dict(
        sorted(get_transform_mapping_dir_items(transform_dir)),
    )


def save_transform_object(ldb_dir: Path, workspace_path: Path) -> str:
    transform_obj = transform_dir_to_object(
        workspace_path / WorkspacePath.TRANSFORM_MAPPING,
    )
    transform_obj_bytes = json_dumps(transform_obj).encode()
    transform_hash = hash_data(transform_obj_bytes)
    transform_path = get_hash_path(
        ldb_dir / InstanceDir.TRANSFORM_MAPPINGS,
        transform_hash,
    )
    write_data_file(
        transform_path,
        transform_obj_bytes,
        overwrite_existing=False,
    )
    return transform_hash

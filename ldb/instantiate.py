import json
import os
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Collection,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

import jmespath
from funcy.objects import cached_property

from ldb.add import process_args_for_delete
from ldb.data_formats import INSTANTIATE_FORMATS, Format
from ldb.dataset import OpDef, apply_queries_to_collection, get_annotation
from ldb.exceptions import LDBException
from ldb.fs.utils import FSProtocol, first_protocol
from ldb.path import InstanceDir, WorkspacePath
from ldb.progress import get_progressbar
from ldb.storage import StorageLocation, get_filesystem, get_storage_locations
from ldb.transform import (
    DEFAULT,
    TransformInfo,
    TransformType,
    get_transform_infos_from_dir,
)
from ldb.typing import JSONDecoded, JSONObject
from ldb.utils import (
    DATA_OBJ_ID_PREFIX,
    get_hash_path,
    json_dumps,
    load_data_file,
    make_target_dir,
    write_data_file,
)
from ldb.workspace import (
    collection_dir_to_object,
    ensure_path_is_empty_workspace,
)


@dataclass
class ItemCopyResult:
    data_object: str = ""
    annotation: str = ""


@dataclass
class InstConfig:
    ldb_dir: Path
    storage_locations: Collection[StorageLocation]
    dest_dir: Union[str, Path]
    intermediate_dir: Union[str, Path] = ""
    transform_infos: Mapping[str, Collection[TransformInfo]] = field(
        default_factory=dict,
    )


class InstantiateResult(NamedTuple):
    data_object_paths: Sequence[str]
    annotation_paths: Sequence[str]
    num_data_objects: int
    num_annotations: int


def instantiate(
    ldb_dir: Path,
    workspace_path: Path,
    dest: Path,
    paths: Sequence[str] = (),
    query_args: Iterable[OpDef] = (),
    fmt: str = Format.BARE,
    force: bool = False,
    apply: Sequence[str] = (),
    make_parent_dirs: bool = False,
    warn: bool = True,
) -> InstantiateResult:
    if fmt not in INSTANTIATE_FORMATS:
        raise ValueError(f"Not a valid instantiation format: {fmt}")

    make_target_dir(dest, parents=make_parent_dirs)
    data_object_hashes: Set[str] = set(
        process_args_for_delete(
            ldb_dir,
            paths,
        ),
    )
    orig_collection = collection_dir_to_object(
        workspace_path / WorkspacePath.COLLECTION,
    )
    collection = {
        d: a if a is not None else ""
        for d, a in orig_collection.items()
        if d in data_object_hashes
    }
    collection = dict(
        apply_queries_to_collection(
            ldb_dir,
            collection.items(),
            query_args,
            warn=warn,
        ),
    )
    return instantiate_collection(
        ldb_dir,
        workspace_path,
        collection,
        dest,
        fmt,
        force,
        apply,
    )


def instantiate_collection(
    ldb_dir: Path,
    workspace_path: Path,
    collection: Mapping[str, Optional[str]],
    dest: Path,
    fmt: str = Format.BARE,
    force: bool = False,
    apply: Sequence[str] = (),
    clean: bool = True,
) -> InstantiateResult:
    try:
        fmt = INSTANTIATE_FORMATS[fmt]
    except KeyError as exc:
        raise ValueError(f"Not a valid instantiation format: {fmt}") from exc
    # fail fast if workspace is not empty
    if clean and dest.exists():
        ensure_path_is_empty_workspace(dest, force)
    storage_locations = get_storage_locations(ldb_dir)
    if fmt in (Format.STRICT, Format.BARE):
        transform_infos = get_transform_infos_from_dir(
            ldb_dir,
            workspace_path / WorkspacePath.TRANSFORM_MAPPING,
        )
    else:
        transform_infos = {}
    dest.mkdir(exist_ok=True)

    tmp_dir_base = workspace_path.absolute() / WorkspacePath.TMP
    tmp_dir_base.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=tmp_dir_base) as final_tmp_dir:
        with tempfile.TemporaryDirectory(dir=tmp_dir_base) as raw_tmp_dir:
            config = InstConfig(
                ldb_dir=ldb_dir,
                dest_dir=final_tmp_dir,
                intermediate_dir=raw_tmp_dir,
                storage_locations=storage_locations,
                transform_infos=transform_infos,
            )
            result = instantiate_collection_directly(
                config,
                collection,
                fmt,
            )
        if apply:
            paths = [str(ldb_dir / InstanceDir.USER_FILTERS)]
            apply_transform(apply, final_tmp_dir, os.fspath(dest), paths=paths)
        else:
            # check again to make sure nothing was added while writing to the
            # temporary location
            if clean:
                ensure_path_is_empty_workspace(dest, force)
            dest_str = os.fspath(dest)
            for path in Path(final_tmp_dir).iterdir():
                os.replace(os.fspath(path), os.path.join(dest_str, path.name))

    return result


def instantiate_collection_directly(
    config: InstConfig,
    collection: Mapping[str, Optional[str]],
    fmt: str,
) -> InstantiateResult:
    fmt = INSTANTIATE_FORMATS[fmt]
    if fmt in (Format.STRICT, Format.BARE):
        return copy_pairs(
            config,
            collection,
            fmt == Format.STRICT,
        )
    if fmt == Format.ANNOT:
        return copy_annot(
            config,
            collection,
        )
    if fmt == Format.INFER:
        return copy_infer(
            config,
            collection,
        )
    if fmt == Format.LABEL_STUDIO:
        return copy_label_studio(
            config,
            collection,
        )
    raise ValueError(f"Not a valid instantiation format: {fmt}")


def apply_transform(
    proc_args: Sequence[str],
    input_dir: str,
    output_dir: str,
    paths: Sequence[str] = (),
) -> int:
    from ldb.pipe import open_plugin  # pylint: disable=import-outside-toplevel

    data = json.dumps(
        [
            os.path.abspath(input_dir),
            os.path.abspath(output_dir),
        ],
    )
    with open_plugin(proc_args, paths, set_cwd=True) as proc:
        stdout, stderr = proc.communicate(data)
        retcode = proc.poll() or 0
        if retcode:
            raise subprocess.CalledProcessError(
                retcode,
                proc.args,
                output=stdout,
                stderr=stderr,
            )
    return retcode


@dataclass
class InstItem:
    config: InstConfig
    data_object_hash: str

    @property
    def dest_dir(self) -> Union[str, Path]:
        return self.config.dest_dir

    @cached_property
    def data_object_meta(self) -> JSONObject:
        data_object_dir = get_hash_path(
            self.config.ldb_dir / InstanceDir.DATA_OBJECT_INFO,
            self.data_object_hash,
        )
        return load_data_file(data_object_dir / "meta")  # type: ignore[no-any-return] # noqa: E501

    @cached_property
    def _prefix_ext(self) -> Tuple[str, str]:
        return get_prefix_ext(self.data_object_meta["fs"]["path"])

    @property
    def prefix(self) -> str:
        return self._prefix_ext[0]

    @property
    def ext(self) -> str:
        return self._prefix_ext[1]

    @cached_property
    def base_dest(self) -> str:
        return os.path.join(
            self.dest_dir,
            self.prefix + self.data_object_hash,
        )

    @cached_property
    def data_object_dest(self) -> str:
        return self.base_dest + self.ext.lower()

    def copy_data_object(
        self,
    ) -> str:
        fs_protocol: FSProtocol = self.data_object_meta["fs"]["protocol"]
        protocol: str = first_protocol(fs_protocol)
        path: str = self.data_object_meta["fs"]["path"]
        fs = get_filesystem(path, protocol, self.config.storage_locations)
        os.makedirs(os.path.split(self.data_object_dest)[0], exist_ok=True)
        dest = self.data_object_dest

        fs.get_file(path, dest)
        return dest

    def copy_files(self) -> ItemCopyResult:
        return ItemCopyResult(data_object=self.copy_data_object())

    def instantiate(self) -> ItemCopyResult:
        return self.copy_files()


def pipe_to_proc(
    data: str,
    proc_args: Sequence[str],
    paths: Sequence[str] = (),
    set_cwd: bool = True,
) -> int:
    from ldb.pipe import open_plugin  # pylint: disable=import-outside-toplevel

    with open_plugin(proc_args, paths, set_cwd=set_cwd) as proc:
        stdout, stderr = proc.communicate(data)
        retcode = proc.poll() or 0
        if retcode:
            raise subprocess.CalledProcessError(
                retcode,
                proc.args,
                output=stdout,
                stderr=stderr,
            )
    return retcode


@dataclass
class RawPairInstItem(InstItem):
    annotation_hash: str

    @cached_property
    def annotation_content(self) -> JSONDecoded:
        return get_annotation(self.config.ldb_dir, self.annotation_hash)

    @cached_property
    def annotation_content_bytes(self) -> bytes:
        return serialize_annotation(self.annotation_content).encode()

    @cached_property
    def annotation_dest(self) -> str:
        return self.base_dest + ".json"

    def copy_annotation(self) -> str:
        dest = self.annotation_dest
        os.makedirs(os.path.split(dest)[0], exist_ok=True)
        write_data_file(
            Path(dest),
            self.annotation_content_bytes,
        )
        return dest

    def copy_files(self) -> ItemCopyResult:
        if self.annotation_hash:
            annotation_path = self.copy_annotation()
        else:
            annotation_path = ""
        return ItemCopyResult(
            data_object=self.copy_data_object(),
            annotation=annotation_path,
        )


@dataclass
class PairInstItem(RawPairInstItem):
    transform_infos: Collection[TransformInfo]

    @property
    def dest_dir(self) -> Union[str, Path]:
        return self.config.intermediate_dir

    def instantiate(self) -> ItemCopyResult:
        copy_result = self.copy_files()
        data = {
            "output_dir": self.config.dest_dir,
        }
        if copy_result.data_object:
            data["data_object"] = copy_result.data_object
        if copy_result.annotation:
            data["annotation"] = copy_result.annotation

        # TODO: put this transform application in separate functions
        for info in self.transform_infos:
            if info.transform.transform_type == TransformType.PREDEFINED:
                if info.transform.value == "self":
                    if copy_result.data_object:
                        shutil.copy2(
                            copy_result.data_object,
                            os.path.join(
                                self.config.dest_dir,
                                os.path.basename(copy_result.data_object),
                            ),
                        )
                    if copy_result.annotation:
                        shutil.copy2(
                            copy_result.annotation,
                            os.path.join(
                                self.config.dest_dir,
                                os.path.basename(copy_result.annotation),
                            ),
                        )
                else:
                    raise ValueError(
                        "Builtin transform does not exist: "
                        f"{info.transform.value}",
                    )
            elif info.transform.transform_type == TransformType.EXEC:
                if not isinstance(info.transform.value, Sequence):
                    raise ValueError(
                        "value must be a list for transform of type "
                        f"{TransformType.EXEC}, "
                        f"got {type(info.transform.value)}",
                    )
                data_str = json.dumps(
                    {"transform_name": info.name, **data},
                )
                pipe_to_proc(data_str, info.transform.value)
            else:
                raise ValueError(
                    f"Invalid transform type: {info.transform.transform_type}",
                )
        return copy_result


@dataclass
class AnnotationOnlyInstItem(RawPairInstItem):
    @cached_property
    def annotation_content(self) -> JSONDecoded:
        annotation: JSONObject = get_annotation(  # type: ignore[assignment]
            self.config.ldb_dir,
            self.annotation_hash,
        )
        annotation = {
            "ldb_meta": {"data_object_id": self.data_object_hash},
            "annotation": annotation,
        }
        return annotation

    def copy_files(self) -> ItemCopyResult:
        return ItemCopyResult(annotation=self.copy_annotation())


@dataclass
class InferInstItem(RawPairInstItem):
    annotation_hash: str

    @cached_property
    def base_dest(self) -> str:
        parts: List[str] = []
        try:
            label = self.annotation_content["label"]  # type: ignore[index, call-overload] # noqa: E501
        except Exception as exc:
            raise LDBException(
                "Annotations for tensorflow-inferred format should contain "
                '"label" key',
            ) from exc
        key: str
        while isinstance(label, dict):
            key, label = next(iter(label.items()))
            parts.append(key)
        parts.append(label)
        return os.path.join(
            self.dest_dir,
            *parts,
            self.prefix + self.data_object_hash,
        )

    def copy_files(self) -> ItemCopyResult:
        return ItemCopyResult(data_object=self.copy_data_object())


@dataclass
class LabelStudioInstItem(RawPairInstItem):
    _annotation_content: List[JSONObject]

    def __init__(
        self,
        config: InstConfig,
        annotation_content: List[JSONObject],
    ):
        super().__init__(
            config=config,
            data_object_hash="",
            annotation_hash="",
        )
        self._annotation_content = annotation_content

    @cached_property
    def annotation_content(self) -> JSONDecoded:
        return self._annotation_content

    @cached_property
    def base_dest(self) -> str:
        return os.path.join(
            self.dest_dir,
            "annotations",
        )

    def copy_files(self) -> ItemCopyResult:
        return ItemCopyResult(annotation=self.copy_annotation())


def serialize_annotation(annotation: JSONDecoded) -> str:
    return json_dumps(annotation, indent=2)


def get_prefix_ext(path: str) -> Tuple[str, str]:
    prefix = path.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    prefix, ext = os.path.splitext(prefix)
    prefix = prefix.replace(".", "-") + "-"
    return prefix, ext


def instantiate_items(
    items: Collection[InstItem],
) -> Tuple[List[str], List[str]]:
    with ThreadPoolExecutor(max_workers=4 * (os.cpu_count() or 1)) as pool:
        with get_progressbar(transient=True) as progress:
            task = progress.add_task("Instantiate", total=len(items))

            def worker(item: InstItem) -> ItemCopyResult:
                result = item.instantiate()
                progress.update(task, advance=1)
                return result

            copy_results = list(pool.map(worker, items))
    data_obj_paths = [r.data_object for r in copy_results]
    annot_paths = [r.annotation for r in copy_results]
    return data_obj_paths, annot_paths


def copy_pairs(
    config: InstConfig,
    collection: Mapping[str, Optional[str]],
    strict: bool = False,
) -> InstantiateResult:
    items: List[Union[PairInstItem, RawPairInstItem]] = []
    num_annotations = 0
    for data_object_hash, annotation_hash in collection.items():
        if annotation_hash:
            num_annotations += 1
        else:
            if strict:
                continue
            annotation_hash = ""
        if config.transform_infos:
            item: Union[PairInstItem, RawPairInstItem] = PairInstItem(
                config,
                data_object_hash,
                annotation_hash,
                config.transform_infos.get(data_object_hash, DEFAULT),
            )
        else:
            item = RawPairInstItem(
                config,
                data_object_hash,
                annotation_hash,
            )
        items.append(item)
    data_obj_paths, annot_paths = instantiate_items(items)
    return InstantiateResult(
        data_obj_paths,
        annot_paths,
        len(data_obj_paths),
        num_annotations,
    )


def copy_annot(
    config: InstConfig,
    collection: Mapping[str, Optional[str]],
) -> InstantiateResult:
    items = []
    for data_object_hash, annotation_hash in collection.items():
        if annotation_hash:
            items.append(
                AnnotationOnlyInstItem(
                    config,
                    data_object_hash,
                    annotation_hash,
                ),
            )
    _, annot_paths = instantiate_items(items)
    return InstantiateResult(
        [],
        annot_paths,
        0,
        len(annot_paths),
    )


def copy_infer(
    config: InstConfig,
    collection: Mapping[str, Optional[str]],
) -> InstantiateResult:
    for data_object_hash, annotation_hash in collection.items():
        if not annotation_hash:
            raise LDBException(
                "For tensorflow-inferred instantiate format, "
                "all data objects must have an annotation. "
                "Missing annotation for data object: "
                f"{DATA_OBJ_ID_PREFIX}{data_object_hash}",
            )
    items = []
    for data_object_hash, annotation_hash in cast(
        Mapping[str, str],
        collection,
    ).items():
        items.append(
            InferInstItem(
                config,
                data_object_hash,
                annotation_hash,
            ),
        )
    data_obj_paths, _ = instantiate_items(items)
    return InstantiateResult(
        data_obj_paths,
        [],
        len(data_obj_paths),
        0,
    )


def copy_label_studio(
    config: InstConfig,
    collection: Mapping[str, Optional[str]],
) -> InstantiateResult:
    ldb_dir = config.ldb_dir
    annot_paths = []
    annotations: List[JSONObject] = []
    for data_object_hash, annotation_hash in collection.items():
        if not annotation_hash:
            raise LDBException(
                "For label-studio instantiate format, "
                "all data objects must have an annotation. "
                "Missing annotation for data object: "
                f"{DATA_OBJ_ID_PREFIX}{data_object_hash}",
            )
        annot = get_annotation(ldb_dir, annotation_hash)
        try:
            annot["data"]["data-object-info"]["md5"]  # type: ignore[call-overload, index] # pylint: disable=pointless-statement # noqa: E501
            path_key = annot["data"]["data-object-info"]["path_key"]  # type: ignore[call-overload, index] # noqa: E501
        except Exception as exc:
            raise LDBException(
                "Malformatted annotation for data object: "
                f"{DATA_OBJ_ID_PREFIX}{data_object_hash}\n"
                "For label-studio instantiation format, "
                "annotations must have the following keys:\n"
                "  data.data-object-info.md5\n"
                "  data.data-object-info.path_key",
            ) from exc
        if not isinstance(jmespath.search(path_key, annot), str):
            raise LDBException(
                "Malformatted annotation for data object: "
                f"{DATA_OBJ_ID_PREFIX}{data_object_hash}\n"
                f"Expected a string at {path_key}, the path key indicated by "
                "data.data-object-info.path_key",
            )
        annotations.append(annot)  # type: ignore[arg-type]
    path = (
        LabelStudioInstItem(
            config,
            annotations,
        )
        .instantiate()
        .annotation
    )
    annot_paths.append(path)
    return InstantiateResult(
        [],
        annot_paths,
        0,
        len(annotations),
    )

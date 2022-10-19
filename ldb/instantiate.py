import json
import os
import shutil
import subprocess
import tempfile
import warnings
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Collection,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import fsspec
import jmespath
from fsspec.utils import get_protocol
from funcy.objects import cached_property

from ldb.add import TransformInfoMapping, paths_to_dataset
from ldb.core import LDBClient
from ldb.data_formats import INSTANTIATE_FORMATS, Format
from ldb.dataset import OpDef
from ldb.exceptions import LDBException, WorkspaceError
from ldb.fs.utils import FSProtocol, first_protocol, unstrip_protocol
from ldb.index.annotation_only import AnnotOnlyParamConfig
from ldb.index.inferred import InferredParamConfig
from ldb.params import ParamConfig
from ldb.path import InstanceDir, WorkspacePath
from ldb.progress import get_progressbar
from ldb.storage import StorageLocation, get_filesystem, get_storage_locations
from ldb.transform import DEFAULT, TransformInfo, TransformType
from ldb.typing import JSONDecoded, JSONObject
from ldb.utils import (
    DATA_OBJ_ID_PREFIX,
    delete_file,
    json_dumps,
    make_target_dir,
    write_data_file,
)
from ldb.workspace import ensure_path_is_empty_workspace


@dataclass
class ItemCopyResult:
    data_object: str = ""
    annotation: str = ""


@dataclass
class InstConfig:
    client: LDBClient
    storage_locations: Collection[StorageLocation]
    dest_dir: Union[str, Path]
    intermediate_dir: Union[str, Path] = ""
    transform_infos: Mapping[str, Collection[TransformInfo]] = field(
        default_factory=dict,
    )
    params: Mapping[str, Any] = field(default_factory=dict)
    add_path: Optional[Union[str, Path]] = None


class InstantiateResult(NamedTuple):
    data_object_paths: Sequence[str]
    annotation_paths: Sequence[str]
    num_data_objects: int
    num_annotations: int

    def num_data_objects_succeeded(self) -> int:
        return sum(bool(d) for d in self.data_object_paths)

    def num_annotations_succeeded(self) -> int:
        return sum(bool(a) for a in self.annotation_paths)


def get_processed_params(
    params: Optional[Mapping[str, str]] = None,
    fmt: str = Format.BARE,
) -> Mapping[str, Any]:
    try:
        fmt = INSTANTIATE_FORMATS[fmt]
    except KeyError as exc:
        raise ValueError(f"Not a valid instantiation format: {fmt}") from exc
    if params is None:
        params = {}
    if fmt == Format.INFER:
        return InferredParamConfig().process_params(params, fmt)
    if fmt == Format.ANNOT:
        return AnnotOnlyParamConfig().process_params(params, fmt)
    return ParamConfig().process_params(params, fmt)


def instantiate(
    ldb_dir: Path,
    dest: str,
    paths: Sequence[str] = (),
    query_args: Iterable[OpDef] = (),
    fmt: str = Format.BARE,
    force: bool = False,
    apply: Sequence[str] = (),
    make_parent_dirs: bool = False,
    warn: bool = True,
    params: Optional[Mapping[str, str]] = None,
) -> InstantiateResult:
    from ldb.core import LDBClient

    client = LDBClient(ldb_dir)
    try:
        fmt = INSTANTIATE_FORMATS[fmt]
    except KeyError as exc:
        raise ValueError(f"Not a valid instantiation format: {fmt}") from exc
    if params is None:
        params = {}

    dest_protocol = get_protocol(dest)
    if dest_protocol == "file":
        remote_fs = None
        local_dest_path = Path(dest)
    else:
        remote_fs = fsspec.filesystem(dest_protocol)
        try:
            if remote_fs.isfile(dest) or remote_fs.ls(dest):
                raise WorkspaceError(
                    f"Not a workspace or an empty directory: {dest}",
                )
        except OSError:
            pass
        local_tmp_dir = tempfile.TemporaryDirectory(  # pylint: disable=consider-using-with
            prefix="ldb-",
        )
        local_dest_path = Path(local_tmp_dir.name)
        print(f"Using local temp dir: {local_tmp_dir.name}")

    processed_params = get_processed_params(params, fmt)

    if remote_fs is None:
        make_target_dir(dest, parents=make_parent_dirs)
    collection, transform_infos = paths_to_dataset(
        client,
        paths,
        query_args,
        warn=warn,
        include_transforms=fmt in (Format.STRICT, Format.BARE, Format.ANNOT, Format.INFER),
    )
    result = instantiate_collection(
        client,
        dict(collection),
        local_dest_path,
        transform_infos=transform_infos,
        fmt=fmt,
        force=force,
        apply=apply,
        params=processed_params,
    )
    if remote_fs is not None:
        print(f"Transferring to remote path: {dest}")
        remote_fs.put(os.fspath(local_dest_path), dest, recursive=True)
    return result


def instantiate_collection(
    client: LDBClient,
    collection: Mapping[str, Optional[str]],
    dest: Path,
    transform_infos: Optional[TransformInfoMapping] = None,
    fmt: str = Format.BARE,
    force: bool = False,
    apply: Sequence[str] = (),
    clean: bool = True,
    params: Optional[Mapping[str, Any]] = None,
    tmp_dir: Optional[Union[str, Path]] = None,
    add_path: Optional[Union[str, Path]] = None,
) -> InstantiateResult:
    try:
        fmt = INSTANTIATE_FORMATS[fmt]
    except KeyError as exc:
        raise ValueError(f"Not a valid instantiation format: {fmt}") from exc
    default_tmp = dest / WorkspacePath.TMP
    if tmp_dir is None:
        tmp_dir = default_tmp
    tmp_dir = os.path.abspath(tmp_dir)
    if params is None:
        params = {}
    # fail fast if workspace is not empty
    if clean and dest.exists():
        ensure_path_is_empty_workspace(dest, force)
    storage_locations = get_storage_locations(client.ldb_dir)
    dest.mkdir(exist_ok=True)

    try:
        os.makedirs(tmp_dir, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=tmp_dir) as final_tmp_dir:
            with tempfile.TemporaryDirectory(dir=tmp_dir) as raw_tmp_dir:
                config = InstConfig(
                    client=client,
                    dest_dir=final_tmp_dir,
                    intermediate_dir=raw_tmp_dir,
                    storage_locations=storage_locations,
                    transform_infos=transform_infos or {},
                    params=params,
                    add_path=add_path,
                )
                result = instantiate_collection_directly(
                    config,
                    collection,
                    fmt,
                )
            if apply:
                paths = [str(client.ldb_dir / InstanceDir.USER_TRANSFORMS)]
                apply_transform(
                    apply,
                    final_tmp_dir,
                    os.fspath(dest),
                    paths=paths,
                )
            else:
                # check again to make sure nothing was added while writing to
                # the temporary location
                if clean:
                    ensure_path_is_empty_workspace(dest, force)
                dest_str = os.fspath(dest)
                for path in Path(final_tmp_dir).iterdir():
                    os.replace(
                        os.fspath(path),
                        os.path.join(dest_str, path.name),
                    )
    finally:
        with suppress(OSError):
            os.rmdir(default_tmp)
            os.rmdir(os.path.dirname(default_tmp))
    return result


def deinstantiate_collection(
    client: LDBClient,
    collection: Mapping[str, Optional[str]],
    dest: Path,
    transform_infos: Optional[TransformInfoMapping] = None,
    fmt: str = Format.BARE,
    params: Optional[Mapping[str, Any]] = None,
) -> InstantiateResult:
    try:
        fmt = INSTANTIATE_FORMATS[fmt]
    except KeyError as exc:
        raise ValueError(f"Not a valid instantiation format: {fmt}") from exc
    if params is None:
        params = {}
    storage_locations = get_storage_locations(client.ldb_dir)

    config = InstConfig(
        client=client,
        dest_dir=dest,
        intermediate_dir=dest,
        storage_locations=storage_locations,
        transform_infos=transform_infos or {},
        params=params,
    )
    result = instantiate_collection_directly(
        config,
        collection,
        fmt,
        deinstantiate=True,
    )
    return result


def instantiate_collection_directly(
    config: InstConfig,
    collection: Mapping[str, Optional[str]],
    fmt: str,
    deinstantiate: bool = False,
) -> InstantiateResult:
    fmt = INSTANTIATE_FORMATS[fmt]
    if fmt in (Format.STRICT, Format.BARE):
        return copy_pairs(config, collection, fmt == Format.STRICT, deinstantiate)
    if fmt == Format.ANNOT:
        single_file = config.params.get("single-file", False)
        if single_file:
            return copy_single_annot(
                config,
                collection,
                deinstantiate,
            )
        return copy_annot(
            config,
            collection,
            deinstantiate,
        )
    if fmt == Format.INFER:
        return copy_infer(
            config,
            collection,
            deinstantiate,
        )
    if deinstantiate:
        warnings.warn(
            f"Deinstantiation not implemented for format: {fmt}",
            RuntimeWarning,
            stacklevel=2,
        )
        return InstantiateResult([], [], 0, 0)
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
    from ldb.pipe import open_plugin

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
        return self.config.client.db.get_data_object_meta(self.data_object_hash)[1]

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
        prefix = self.prefix
        obj_id = self.data_object_hash
        name = prefix if prefix.endswith(obj_id) else f"{prefix}-{obj_id}"
        return os.path.join(
            self.dest_dir,
            name,
        )

    @cached_property
    def data_object_dest(self) -> str:
        return self.base_dest + self.ext.lower()

    def copy_data_object(self) -> str:
        fs_protocol: FSProtocol = self.data_object_meta["fs"]["protocol"]
        protocol: str = first_protocol(fs_protocol)
        path: str = self.data_object_meta["fs"]["path"]
        fs = get_filesystem(path, protocol, self.config.storage_locations)
        os.makedirs(os.path.split(self.data_object_dest)[0], exist_ok=True)
        dest = self.data_object_dest

        try:
            fs.get_file(path, dest)
        except FileNotFoundError as exc:
            full_path = unstrip_protocol(fs, path)
            if exc.args and full_path in exc.args:
                raise
            raise FileNotFoundError(full_path) from exc
        return dest

    def copy_files(self) -> ItemCopyResult:
        return ItemCopyResult(data_object=self.copy_data_object())

    def instantiate(self) -> ItemCopyResult:
        return self.copy_files()

    def delete_data_object(self) -> str:
        dest = self.data_object_dest
        if delete_file(dest):
            return dest
        return ""

    def delete_files(self) -> ItemCopyResult:
        return ItemCopyResult(data_object=self.delete_data_object())

    def deinstantiate(self) -> ItemCopyResult:
        return self.delete_files()


def pipe_to_proc(
    data: str,
    proc_args: Sequence[str],
    paths: Sequence[str] = (),
    set_cwd: bool = True,
) -> int:
    from ldb.pipe import open_plugin

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
        return self.config.client.db.get_annotation(self.annotation_hash)[1]

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

    def delete_annotation_file(self) -> str:
        dest = self.annotation_dest
        if delete_file(dest):
            return dest
        return ""

    def delete_files(self) -> ItemCopyResult:
        if self.annotation_hash:
            annotation_path = self.delete_annotation_file()
        else:
            annotation_path = ""
        return ItemCopyResult(
            data_object=self.delete_data_object(),
            annotation=annotation_path,
        )


@dataclass
class PairInstItem(RawPairInstItem):
    transform_infos: Collection[TransformInfo]

    @property
    def dest_dir(self) -> Union[str, Path]:
        return self.config.intermediate_dir

    def instantiate(self) -> ItemCopyResult:
        # TODO: include in copy_result transform output files, not just
        # raw instantiated files
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
                        "Builtin transform does not exist: " f"{info.transform.value}",
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
                pipe_to_proc(
                    data_str,
                    info.transform.value,
                    paths=[
                        str(self.config.client.ldb_dir / InstanceDir.USER_TRANSFORMS),
                    ],
                    set_cwd=True,
                )
            else:
                raise ValueError(
                    f"Invalid transform type: {info.transform.transform_type}",
                )
        return copy_result

    # TODO: Add deinstantiate for transformed files here


@dataclass
class AnnotationOnlyInstItem(RawPairInstItem):
    transform_infos: Optional[Collection[TransformInfo]] = None

    @cached_property
    def annotation_content(self) -> JSONDecoded:
        annotation: JSONObject = self.config.client.db.get_annotation(  # type: ignore[assignment]
            self.annotation_hash
        )
        fs_protocol: FSProtocol = self.data_object_meta["fs"]["protocol"]
        protocol: str = first_protocol(fs_protocol)
        path: str = self.data_object_meta["fs"]["path"]
        fs = get_filesystem(path, protocol, self.config.storage_locations)
        path = unstrip_protocol(fs, path)
        annotation = {
            "data-object-info": {
                "md5": self.data_object_hash,
                "path": path,
            },
            "annotation": annotation,
        }
        if self.transform_infos:
            annotation["ldb-meta"] = {
                "transforms": [t.to_dict() for t in self.transform_infos],
            }
        return annotation

    def copy_files(self) -> ItemCopyResult:
        return ItemCopyResult(annotation=self.copy_annotation())

    def delete_files(self) -> ItemCopyResult:
        return ItemCopyResult(annotation=self.delete_annotation_file())


@dataclass
class SingleAnnotationInstItem(AnnotationOnlyInstItem):
    annotation_list: List[JSONDecoded] = field(default_factory=list)

    def copy_annotation(self) -> str:
        self.annotation_list.append(self.annotation_content)
        return ""

    def delete_annotation_file(self) -> str:
        self.annotation_list.append(self.annotation_content)
        return ""


@dataclass
class InferInstItem(RawPairInstItem):
    annotation_hash: str
    label_key: Sequence[str]
    base_label: str = ""

    @cached_property
    def base_dest(self) -> str:
        parts = infer_dir(
            self.annotation_content,
            self.label_key,
            self.base_label,
        )
        prefix = self.prefix
        obj_id = self.data_object_hash
        name = prefix if prefix.endswith(obj_id) else f"{prefix}-{obj_id}"
        return os.path.join(
            self.dest_dir,
            *parts,
            name,
        )

    def copy_files(self) -> ItemCopyResult:
        return ItemCopyResult(data_object=self.copy_data_object())

    def delete_files(self) -> ItemCopyResult:
        return ItemCopyResult(data_object=self.delete_data_object())


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
    prefix = prefix.replace(".", "-")
    return prefix, ext


def instantiate_items(
    items: Collection[InstItem],
    deinstantiate: bool = False,
) -> Tuple[List[str], List[str]]:
    with ThreadPoolExecutor(max_workers=4 * (os.cpu_count() or 1)) as pool:
        with get_progressbar(transient=True) as progress:
            task = progress.add_task(
                "Deinstantiate" if deinstantiate else "Instantiate", total=len(items)
            )

            def worker(item: InstItem) -> ItemCopyResult:
                if deinstantiate:
                    result = item.deinstantiate()
                else:
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
    deinstantiate: bool = False,
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
    data_obj_paths, annot_paths = instantiate_items(items, deinstantiate=deinstantiate)
    return InstantiateResult(
        data_obj_paths,
        annot_paths,
        len(data_obj_paths),
        num_annotations,
    )


def copy_annot(
    config: InstConfig,
    collection: Mapping[str, Optional[str]],
    deinstantiate: bool = False,
) -> InstantiateResult:
    items = []
    for data_object_hash, annotation_hash in collection.items():
        if annotation_hash:
            items.append(
                AnnotationOnlyInstItem(
                    config,
                    data_object_hash,
                    annotation_hash,
                    config.transform_infos.get(data_object_hash),
                ),
            )
    _, annot_paths = instantiate_items(items, deinstantiate)
    return InstantiateResult(
        [],
        annot_paths,
        0,
        len(annot_paths),
    )


def copy_single_annot(
    config: InstConfig,
    collection: Mapping[str, Optional[str]],
    deinstantiate: bool = False,
) -> InstantiateResult:
    items = []
    annotation_list: List[JSONDecoded] = []
    for data_object_hash, annotation_hash in collection.items():
        if annotation_hash:
            items.append(
                SingleAnnotationInstItem(
                    config,
                    data_object_hash,
                    annotation_hash,
                    config.transform_infos.get(data_object_hash),
                    annotation_list=annotation_list,
                ),
            )
    instantiate_items(items, deinstantiate)
    dest = Path(os.path.join(config.dest_dir, "dataset.json"))
    # TODO: Optimize sync to use modify_single_annot only once
    if config.add_path:
        src = Path(os.path.join(config.add_path, "dataset.json"))
        added_count, _ = modify_single_annot(src, dest, annotation_list, [])
        return InstantiateResult(
            [],
            [],
            0,
            added_count,
        )
    if deinstantiate:
        _, removed_count = modify_single_annot(dest, dest, [], annotation_list)
        return InstantiateResult(
            [],
            ["R" for _ in range(removed_count)],
            0,
            removed_count,
        )
    annotation = serialize_annotation(annotation_list)
    annotation_bytes = annotation.encode()
    write_data_file(dest, annotation_bytes)
    return InstantiateResult(
        [],
        [],
        0,
        len(annotation_list),
    )


def modify_single_annot(
    src: Union[str, Path],
    dest: Union[str, Path],
    add_list: List[JSONDecoded],
    remove_list: List[JSONDecoded],
) -> Tuple[int, int]:
    try:
        with open(src, "rb") as f:
            original_annotations = json.load(f)
    except FileNotFoundError:
        if add_list:
            original_annotations = []
        else:
            return 0, 0
    if not isinstance(original_annotations, list):
        raise LDBException(
            "For modification of the single-file annotation format "
            "the annotation file dataset.json must contain a top-level array."
        )
    try:
        add_dict = {a["data-object-info"]["md5"]: a for a in add_list}  # type: ignore
    except (KeyError, TypeError) as exc:
        raise ValueError("Missing md5 hashes in add_list") from exc
    try:
        remove_hashes_set = {r["data-object-info"]["md5"] for r in remove_list}  # type: ignore # noqa: E501
    except (KeyError, TypeError) as exc:
        raise ValueError("Missing md5 hashes in remove_list") from exc
    try:
        filtered_annotations_dict = {
            o["data-object-info"]["md5"]: o
            for o in original_annotations
            if o["data-object-info"]["md5"] not in remove_hashes_set
        }
    except (KeyError, TypeError) as exc:
        raise ValueError("Missing md5 hashes in dataset.json entries") from exc
    filtered_count = len(filtered_annotations_dict)
    removed_count = len(original_annotations) - filtered_count
    final_annotations = list({**filtered_annotations_dict, **add_dict}.values())
    final_count = len(final_annotations)
    added_count = final_count - filtered_count

    if final_count == 0:
        if not delete_file(dest):
            # Set to zero if delete fails
            removed_count = 0
    else:
        annotation_bytes = serialize_annotation(final_annotations).encode()
        write_data_file(dest, annotation_bytes)
    return added_count, removed_count


def copy_infer(
    config: InstConfig,
    collection: Mapping[str, Optional[str]],
    deinstantiate: bool = False,
) -> InstantiateResult:
    label_key: Sequence[str] = config.params.get("label-key", ["label"])
    base_label: str = config.params.get("base-label", "")
    for data_object_hash, annotation_hash in collection.items():
        if not annotation_hash:
            raise LDBException(
                "For tensorflow-inferred instantiate format, "
                "all data objects must have an annotation. "
                "Missing annotation for data object: "
                f"{DATA_OBJ_ID_PREFIX}{data_object_hash}",
            )
    collection = cast(Mapping[str, str], collection)
    if config.transform_infos:
        if deinstantiate:
            warnings.warn(
                "Deinstantiation not implemented for transformations",
                RuntimeWarning,
                stacklevel=2,
            )
        return _copy_infer_with_transforms(
            config,
            collection,
            label_key,
            base_label,
        )
    return _copy_infer_without_transforms(
        config,
        collection,
        label_key,
        base_label,
        deinstantiate,
    )


def _copy_infer_without_transforms(
    config: InstConfig,
    collection: Mapping[str, str],
    label_key: Sequence[str],
    base_label: str,
    deinstantiate: bool = False,
) -> InstantiateResult:
    items = []
    for data_object_hash, annotation_hash in collection.items():
        items.append(
            InferInstItem(
                config,
                data_object_hash,
                annotation_hash,
                label_key=label_key,
                base_label=base_label,
            ),
        )
    data_obj_paths, _ = instantiate_items(items, deinstantiate)
    return InstantiateResult(
        data_obj_paths,
        [],
        len(data_obj_paths),
        0,
    )


def _copy_infer_with_transforms(
    config: InstConfig,
    collection: Mapping[str, str],
    label_key: Sequence[str],
    base_label: str,
) -> InstantiateResult:
    result = copy_pairs(
        config,
        collection,
        strict=False,
    )

    collection = {}
    annotation_paths = set()
    data_object_paths = set()
    for path in os.listdir(config.dest_dir):
        if path.endswith(".json"):
            annotation_paths.add(os.path.join(config.dest_dir, path))
        else:
            data_object_paths.add(os.path.join(config.dest_dir, path))

    for path in data_object_paths:
        base, _ = os.path.splitext(path)
        annot_path = f"{base}.json"
        if annot_path in annotation_paths:
            collection[path] = annot_path

    pairs_to_inferred_dirs(
        str(config.dest_dir),
        collection.items(),
        label_key,
        base_label,
    )
    for path in annotation_paths | (data_object_paths - collection.keys()):
        os.unlink(path)
    return result


def pairs_to_inferred_dirs(
    dir_path: str,
    path_mapping: Iterable[Tuple[str, str]],
    label_key: Sequence[str],
    base_label: str = "",
) -> None:
    for data_object_path, annotation_path in path_mapping:
        with open(annotation_path, encoding="utf-8") as f:
            raw_content = f.read()
        annot = json.loads(raw_content)
        path_parts = infer_dir(annot, label_key, base_label)
        dest = os.path.join(
            dir_path,
            *path_parts,
            os.path.basename(data_object_path),
        )
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.move(data_object_path, dest)


def infer_dir(
    annotation: JSONDecoded,
    label_key: Sequence[str],
    base_label: str = "",
) -> List[str]:
    parts: List[str] = []
    key: str
    label: Union[JSONObject, str]
    try:
        label = annotation  # type: ignore[assignment]
        for key in label_key:
            label = label[key]  # type: ignore[index]
    except Exception as exc:
        # TODO: convert self.label_key to jmespath expression for message
        raise LDBException(
            "Annotations for tensorflow-inferred format should contain " f"key: {label_key}",
        ) from exc

    if label != base_label:
        while isinstance(label, dict):
            key, label = next(iter(label.items()))  # type: ignore[assignment]
            parts.append(key)
        parts.append(label)
    return parts


def copy_label_studio(
    config: InstConfig,
    collection: Mapping[str, Optional[str]],
) -> InstantiateResult:
    ldb_dir = config.client.ldb_dir
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
        annot = config.client.db.get_annotation(annotation_hash)
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
    path = LabelStudioInstItem(config, annotations).instantiate().annotation
    annot_paths.append(path)
    return InstantiateResult(
        [],
        annot_paths,
        0,
        len(annotations),
    )

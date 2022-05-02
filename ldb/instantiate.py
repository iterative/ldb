import json
import os
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Collection,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from funcy.objects import cached_property

from ldb.data_formats import INSTANTIATE_FORMATS, Format
from ldb.dataset import get_annotation
from ldb.exceptions import LDBException
from ldb.fs.utils import FSProtocol, first_protocol
from ldb.path import InstanceDir, WorkspacePath
from ldb.progress import get_progressbar
from ldb.storage import StorageLocation, get_filesystem, get_storage_locations
from ldb.typing import JSONDecoded, JSONObject
from ldb.utils import (
    DATA_OBJ_ID_PREFIX,
    get_hash_path,
    json_dumps,
    load_data_file,
    write_data_file,
)
from ldb.workspace import (
    collection_dir_to_object,
    ensure_path_is_empty_workspace,
)


class InstantiateResult(NamedTuple):
    data_object_paths: List[str]
    annotation_paths: List[str]
    num_data_objects: int
    num_annotations: int


def instantiate(
    ldb_dir: Path,
    workspace_path: Path,
    dest: Path,
    fmt: str = Format.BARE,
    force: bool = False,
    apply: Sequence[str] = (),
) -> InstantiateResult:
    fmt = INSTANTIATE_FORMATS[fmt]
    collection = collection_dir_to_object(
        workspace_path / WorkspacePath.COLLECTION,
    )

    # fail fast if workspace is not empty
    if dest.exists():
        ensure_path_is_empty_workspace(dest, force)
    dest.mkdir(exist_ok=True)

    tmp_dir_base = workspace_path / WorkspacePath.TMP
    tmp_dir_base.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=tmp_dir_base) as tmp_dir:
        result = instantiate_collection(
            ldb_dir,
            collection,
            tmp_dir,
            fmt,
        )

        if apply:
            paths = [str(ldb_dir / InstanceDir.USER_FILTERS)]
            apply_transform(apply, tmp_dir, os.fspath(dest), paths=paths)
        else:
            # check again to make sure nothing was added while writing to the
            # temporary location
            ensure_path_is_empty_workspace(dest, force)
            dest_str = os.fspath(dest)
            for path in Path(tmp_dir).iterdir():
                shutil.move(os.fspath(path), dest_str)

    return result


def instantiate_collection(
    ldb_dir: Path,
    collection: Mapping[str, Optional[str]],
    dest_dir: Union[str, Path],
    fmt: str,
) -> InstantiateResult:
    fmt = INSTANTIATE_FORMATS[fmt]
    if fmt in (Format.STRICT, Format.BARE):
        return copy_pairs(
            ldb_dir,
            collection,
            dest_dir,
            fmt == Format.STRICT,
        )
    if fmt == Format.ANNOT:
        return copy_annot(
            ldb_dir,
            collection,
            dest_dir,
        )
    if fmt == Format.INFER:
        return copy_infer(
            ldb_dir,
            collection,
            dest_dir,
        )
    if fmt == Format.LABEL_STUDIO:
        return copy_label_studio(
            ldb_dir,
            collection,
            dest_dir,
        )
    raise ValueError(f"Not a valid indexing format: {fmt}")


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
    ldb_dir: Path
    dest_dir: Union[str, Path]
    data_object_hash: str
    storage_locations: Collection[StorageLocation]

    @cached_property
    def data_object_meta(self) -> JSONObject:
        data_object_dir = get_hash_path(
            self.ldb_dir / InstanceDir.DATA_OBJECT_INFO,
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
        fs = get_filesystem(path, protocol, self.storage_locations)
        os.makedirs(os.path.split(self.data_object_dest)[0], exist_ok=True)
        dest = self.data_object_dest

        fs.get_file(path, dest)
        return dest


@dataclass
class PairInstItem(InstItem):
    annotation_hash: str

    @cached_property
    def annotation_content(self) -> JSONDecoded:
        return get_annotation(self.ldb_dir, self.annotation_hash)

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


@dataclass
class AnnotationOnlyInstItem(PairInstItem):
    @cached_property
    def annotation_content(self) -> JSONDecoded:
        annotation: JSONObject = get_annotation(  # type: ignore[assignment]
            self.ldb_dir,
            self.annotation_hash,
        )
        annotation = {
            "ldb_meta": {"data_object_id": self.data_object_hash},
            "annotation": annotation,
        }
        return annotation


@dataclass
class InferInstItem(PairInstItem):
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


@dataclass
class LabelStudioInstItem(PairInstItem):
    _annotation_content: List[JSONObject]

    def __init__(
        self,
        ldb_dir: Path,
        dest_dir: Union[str, Path],
        annotation_content: List[JSONObject],
        storage_locations: Collection[StorageLocation],
    ):
        super().__init__(
            ldb_dir,
            dest_dir,
            "",
            storage_locations,
            "",
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


def serialize_annotation(annotation: JSONDecoded) -> str:
    return json_dumps(annotation, indent=2)


def get_prefix_ext(path: str) -> Tuple[str, str]:
    prefix = path.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    prefix, ext = os.path.splitext(prefix)
    prefix = prefix.replace(".", "-") + "-"
    return prefix, ext


def copy_pairs(
    ldb_dir: Path,
    collection: Mapping[str, Optional[str]],
    dest_dir: Union[str, Path],
    strict: bool = False,
) -> InstantiateResult:
    items: List[PairInstItem] = []
    data_obj_paths: List[str] = []
    annot_paths = []
    num_annotations = 0
    # annotations are small and stored in ldb; copy them first
    for data_object_hash, annotation_hash in collection.items():
        if annotation_hash is None:
            if strict:
                continue
            annotation_hash = ""
        item = PairInstItem(
            ldb_dir,
            dest_dir,
            data_object_hash,
            get_storage_locations(ldb_dir),
            annotation_hash,
        )
        if annotation_hash:
            annot_paths.append(item.copy_annotation())
            num_annotations += 1
        else:
            annot_paths.append("")
        items.append(item)

    with ThreadPoolExecutor(max_workers=4 * (os.cpu_count() or 1)) as pool:
        with get_progressbar(transient=True) as progress:
            task = progress.add_task("Instantiate", total=len(items))

            def worker(item: PairInstItem) -> str:
                result = item.copy_data_object()
                progress.update(task, advance=1)
                return result

            data_obj_paths = list(pool.map(worker, items))

    return InstantiateResult(
        data_obj_paths,
        annot_paths,
        len(data_obj_paths),
        num_annotations,
    )


def copy_annot(
    ldb_dir: Path,
    collection: Mapping[str, Optional[str]],
    dest_dir: Union[str, Path],
) -> InstantiateResult:
    annot_paths = []
    for data_object_hash, annotation_hash in collection.items():
        if annotation_hash:
            path = AnnotationOnlyInstItem(
                ldb_dir,
                dest_dir,
                data_object_hash,
                get_storage_locations(ldb_dir),
                annotation_hash,
            ).copy_annotation()
            annot_paths.append(path)
    return InstantiateResult(
        [],
        annot_paths,
        0,
        len(annot_paths),
    )


def copy_infer(
    ldb_dir: Path,
    collection: Mapping[str, Optional[str]],
    dest_dir: Union[str, Path],
) -> InstantiateResult:
    for data_object_hash, annotation_hash in collection.items():
        if not annotation_hash:
            raise LDBException(
                "For tensorflow-inferred instantiate format, "
                "all data objects must have an annotation. "
                "Missing annotation for data object: "
                f"{DATA_OBJ_ID_PREFIX}{data_object_hash}",
            )
    data_obj_paths = []
    for data_object_hash, annotation_hash in cast(
        Mapping[str, str],
        collection,
    ).items():
        path = InferInstItem(
            ldb_dir,
            dest_dir,
            data_object_hash,
            get_storage_locations(ldb_dir),
            annotation_hash,
        ).copy_data_object()
        data_obj_paths.append(path)
    return InstantiateResult(
        data_obj_paths,
        [],
        len(data_obj_paths),
        0,
    )


def copy_label_studio(
    ldb_dir: Path,
    collection: Mapping[str, Optional[str]],
    dest_dir: Union[str, Path],
    url_key: str = "image",
) -> InstantiateResult:
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
            annot["data"][url_key]  # type: ignore[call-overload, index]
        except Exception as exc:
            raise LDBException(
                "For label-studio instantiate format, "
                f'annotations must have the key "data.{url_key}." '
                "Malformatted annotation for data object: "
                f"{DATA_OBJ_ID_PREFIX}{data_object_hash}",
            ) from exc
        annotations.append(annot)  # type: ignore[arg-type]
    path = LabelStudioInstItem(
        ldb_dir,
        dest_dir,
        annotations,
        get_storage_locations(ldb_dir),
    ).copy_annotation()
    annot_paths.append(path)
    return InstantiateResult(
        [],
        annot_paths,
        0,
        len(annotations),
    )

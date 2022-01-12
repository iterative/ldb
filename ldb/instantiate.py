import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import List, Mapping, Optional, Tuple, cast

import fsspec
from funcy.objects import cached_property

from ldb.data_formats import INSTANTIATE_FORMATS, Format
from ldb.dataset import get_annotation
from ldb.exceptions import LDBException
from ldb.path import InstanceDir, WorkspacePath
from ldb.typing import JSONDecoded, JSONObject
from ldb.utils import (
    get_hash_path,
    json_dumps,
    load_data_file,
    write_data_file,
)
from ldb.workspace import collection_dir_to_object, ensure_empty_workspace


def instantiate(
    ldb_dir: Path,
    workspace_path: Path,
    fmt: str = Format.BARE,
    force: bool = False,
) -> Tuple[int, int]:
    fmt = INSTANTIATE_FORMATS[fmt]
    collection = collection_dir_to_object(
        workspace_path / WorkspacePath.COLLECTION,
    )

    # fail fast if workspace is not empty
    ensure_empty_workspace(workspace_path, force)

    tmp_dir_base = workspace_path / WorkspacePath.TMP
    tmp_dir_base.mkdir(exist_ok=True)
    tmp_dir = Path(tempfile.mkdtemp(dir=tmp_dir_base))

    if fmt in (Format.STRICT, Format.BARE):
        num_data_objects, num_annotations = copy_pairs(
            ldb_dir,
            collection,
            tmp_dir,
            fmt == Format.STRICT,
        )
    elif fmt == Format.ANNOT:
        num_data_objects, num_annotations = copy_annot(
            ldb_dir,
            collection,
            tmp_dir,
        )
    elif fmt == Format.INFER:
        num_data_objects, num_annotations = copy_infer(
            ldb_dir,
            collection,
            tmp_dir,
        )
    elif fmt == Format.LABEL_STUDIO:
        num_data_objects, num_annotations = copy_label_studio(
            ldb_dir,
            collection,
            tmp_dir,
        )
    else:
        raise ValueError(f"Not a valid indexing format: {fmt}")

    # check again to make sure nothing was added while writing to the
    # temporary location
    ensure_empty_workspace(workspace_path, force)
    for path in tmp_dir.iterdir():
        shutil.move(os.fspath(path), os.fspath(workspace_path))

    tmp_dir.rmdir()
    return num_data_objects, num_annotations


@dataclass
class InstItem:
    ldb_dir: Path
    dest_dir: Path
    data_object_hash: str

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

    def copy_data_object(self) -> None:
        fs = fsspec.filesystem(self.data_object_meta["fs"]["protocol"])
        os.makedirs(os.path.split(self.data_object_dest)[0], exist_ok=True)
        fs.get_file(self.data_object_meta["fs"]["path"], self.data_object_dest)


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

    def copy_annotation(self) -> None:
        os.makedirs(os.path.split(self.annotation_dest)[0], exist_ok=True)
        write_data_file(
            Path(self.annotation_dest),
            self.annotation_content_bytes,
        )


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
        tmp_dir: Path,
        annotation_content: List[JSONObject],
    ):
        super().__init__(
            ldb_dir,
            tmp_dir,
            "",
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
    tmp_dir: Path,
    strict: bool = False,
) -> Tuple[int, int]:
    items = []
    num_annotations = 0
    # annotations are small and stored in ldb; copy them first
    for data_object_hash, annotation_hash in collection.items():
        if annotation_hash is None:
            if strict:
                continue
            annotation_hash = ""
        item = PairInstItem(
            ldb_dir,
            tmp_dir,
            data_object_hash,
            annotation_hash,
        )
        if annotation_hash:
            item.copy_annotation()
            num_annotations += 1
        items.append(item)
    for item in items:
        item.copy_data_object()
    return len(items), num_annotations


def copy_annot(
    ldb_dir: Path,
    collection: Mapping[str, Optional[str]],
    tmp_dir: Path,
) -> Tuple[int, int]:
    num_annotations = 0
    for data_object_hash, annotation_hash in collection.items():
        if annotation_hash:
            AnnotationOnlyInstItem(
                ldb_dir,
                tmp_dir,
                data_object_hash,
                annotation_hash,
            ).copy_annotation()
            num_annotations += 1
    return 0, num_annotations


def copy_infer(
    ldb_dir: Path,
    collection: Mapping[str, Optional[str]],
    tmp_dir: Path,
) -> Tuple[int, int]:
    num_annotations = 0
    for data_object_hash, annotation_hash in collection.items():
        if not annotation_hash:
            raise LDBException(
                "For tensorflow-inferred instantiate format, "
                "all data objects must have an annotation. "
                f"Missing annotation for data object: 0x{data_object_hash}",
            )
    for data_object_hash, annotation_hash in cast(
        Mapping[str, str],
        collection,
    ).items():
        InferInstItem(
            ldb_dir,
            tmp_dir,
            data_object_hash,
            annotation_hash,
        ).copy_data_object()
        num_annotations += 1
    return 0, num_annotations


def copy_label_studio(
    ldb_dir: Path,
    collection: Mapping[str, Optional[str]],
    tmp_dir: Path,
    url_key: str = "image",
) -> Tuple[int, int]:
    num_annotations = 0
    annotations: List[JSONObject] = []
    for data_object_hash, annotation_hash in collection.items():
        if not annotation_hash:
            raise LDBException(
                "For label-studio instantiate format, "
                "all data objects must have an annotation. "
                f"Missing annotation for data object: 0x{data_object_hash}",
            )
        annot = get_annotation(ldb_dir, annotation_hash)
        try:
            annot["data"][url_key]  # type: ignore[call-overload, index]
        except Exception as exc:
            raise LDBException(
                "For label-studio instantiate format, "
                f'annotations must have the key "data.{url_key}." '
                "Malformatted annotation for data object: "
                f"0x{data_object_hash}",
            ) from exc
        annotations.append(annot)  # type: ignore[arg-type]
        num_annotations += 1
    LabelStudioInstItem(
        ldb_dir,
        tmp_dir,
        annotations,
    ).copy_annotation()
    return 0, num_annotations

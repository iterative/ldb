import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional, Tuple

import fsspec
from funcy.objects import cached_property

from ldb.data_formats import FORMATS, Format
from ldb.path import InstanceDir, WorkspacePath
from ldb.typing import JSONDecoded, JSONObject
from ldb.utils import get_hash_path, json_dumps, load_data_file
from ldb.workspace import collection_dir_to_object, ensure_empty_workspace


def instantiate(
    ldb_dir: Path,
    workspace_path: Path,
    fmt: str = Format.BARE,
    force: bool = False,
) -> Tuple[int, int]:
    fmt = FORMATS[fmt]
    collection_obj = collection_dir_to_object(
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
            collection_obj,
            tmp_dir,
            fmt == Format.STRICT,
        )
    elif fmt == Format.ANNOT:
        num_data_objects, num_annotations = copy_annot(
            ldb_dir,
            collection_obj,
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

    @property
    def data_object_dest(self) -> Path:
        return self.dest_dir / (
            self.prefix + self.data_object_hash + self.ext.lower()
        )

    def copy_data_object(self) -> None:
        fs = fsspec.filesystem(self.data_object_meta["fs"]["protocol"])
        fs.get_file(self.data_object_meta["fs"]["path"], self.data_object_dest)


@dataclass
class PairInstItem(InstItem):
    annotation_hash: str

    def annotation_content(self) -> str:
        annotation = get_annotation(self.ldb_dir, self.annotation_hash)
        return serialize_annotation(annotation)

    @property
    def annotation_dest(self) -> Path:
        return self.dest_dir / (self.prefix + self.data_object_hash + ".json")

    def copy_annotation(self) -> None:
        with open(self.annotation_dest, "x", encoding="utf-8") as f:
            f.write(self.annotation_content())


@dataclass
class AnnotationOnlyInstItem(PairInstItem):
    def annotation_content(self) -> str:
        annotation: JSONObject = get_annotation(  # type: ignore[assignment]
            self.ldb_dir,
            self.annotation_hash,
        )
        annotation = {
            "ldb_meta": {"data_object_id": self.data_object_hash},
            "annotation": annotation,
        }
        return serialize_annotation(annotation)


def get_annotation(ldb_dir: Path, annotation_hash: str) -> JSONDecoded:
    user_annotation_file_path = (
        get_hash_path(
            ldb_dir / InstanceDir.ANNOTATIONS,
            annotation_hash,
        )
        / "user"
    )
    with open(user_annotation_file_path, encoding="utf-8") as f:
        data = f.read()
    return json.loads(data)  # type: ignore[no-any-return]


def serialize_annotation(annotation: JSONDecoded) -> str:
    return json_dumps(annotation, indent=2)


def get_prefix_ext(path: str) -> Tuple[str, str]:
    prefix = path.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    prefix, ext = os.path.splitext(prefix)
    prefix = prefix.replace(".", "-") + "-"
    return prefix, ext


def copy_pairs(
    ldb_dir: Path,
    collection_obj: Mapping[str, Optional[str]],
    tmp_dir: Path,
    strict: bool = False,
) -> Tuple[int, int]:
    items = []
    num_annotations = 0
    # annotations are small and stored in ldb; copy them first
    for data_object_hash, annotation_hash in collection_obj.items():
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
    collection_obj: Mapping[str, Optional[str]],
    tmp_dir: Path,
) -> Tuple[int, int]:
    num_annotations = 0
    for data_object_hash, annotation_hash in collection_obj.items():
        if annotation_hash:
            AnnotationOnlyInstItem(
                ldb_dir,
                tmp_dir,
                data_object_hash,
                annotation_hash,
            ).copy_annotation()
            num_annotations += 1
    return 0, num_annotations

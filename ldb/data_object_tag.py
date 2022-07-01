from pathlib import Path
from typing import Collection, Iterable, Optional, Tuple

from ldb.path import InstanceDir
from ldb.utils import (
    get_hash_path,
    json_dumps,
    load_data_file,
    write_data_file,
)


def tag_data_objects(
    ldb_dir: Path,
    data_object_hashes: Iterable[str],
    add_tags: Collection[str] = (),
    remove_tags: Collection[str] = (),
    set_tags: Optional[Collection[str]] = None,
) -> Tuple[int, int]:
    print("Tagging data objects")
    num_selected = 0
    num_changed = 0
    for data_object_hash in data_object_hashes:
        num_changed += tag_data_object(
            ldb_dir,
            data_object_hash,
            add_tags,
            remove_tags,
            set_tags,
        )
        num_selected += 1
    print(
        f"  Data objects: {num_selected:8d}\n"
        f"  Num updated:  {num_changed:8d}",
    )
    return num_selected, num_changed


def tag_data_object(
    ldb_dir: Path,
    data_object_hash: str,
    add_tags: Collection[str] = (),
    remove_tags: Collection[str] = (),
    set_tags: Optional[Collection[str]] = None,
) -> bool:
    meta_path = (
        get_hash_path(
            ldb_dir / InstanceDir.DATA_OBJECT_INFO,
            data_object_hash,
        )
        / "meta"
    )
    meta = load_data_file(meta_path)
    if set_tags is not None:
        tag_set = set(set_tags)
    else:
        tag_set = set(meta["tags"])
    tag_set.update(add_tags)
    tag_set.difference_update(remove_tags)
    result = sorted(tag_set)
    if result != meta["tags"]:
        meta["tags"] = result
        write_data_file(meta_path, json_dumps(meta).encode())
        return True
    return False

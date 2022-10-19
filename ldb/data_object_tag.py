from pathlib import Path
from typing import Collection, Iterable, List, Optional, Tuple

from ldb.core import LDBClient


def tag_data_objects(
    ldb_dir: Path,
    data_object_hashes: Iterable[str],
    add_tags: Collection[str] = (),
    remove_tags: Collection[str] = (),
    set_tags: Optional[Collection[str]] = None,
) -> Tuple[int, int]:
    client = LDBClient(ldb_dir)
    print("Tagging data objects")
    num_selected = 0
    num_changed = 0
    for id, meta in client.db.get_data_object_meta_many(data_object_hashes):
        tags = meta["tags"]
        new_tags = tag_data_object(
            tags,
            add_tags,
            remove_tags,
            set_tags,
        )
        if new_tags != tags:
            meta["tags"] = new_tags
            client.db.add_data_object_meta(id, meta)
            num_changed += 1
        num_selected += 1
    print(f"  Data objects: {num_selected:8d}\n  Num updated:  {num_changed:8d}")
    return num_selected, num_changed


def tag_data_object(
    tags: Collection[str],
    add_tags: Collection[str] = (),
    remove_tags: Collection[str] = (),
    set_tags: Optional[Collection[str]] = None,
) -> List[str]:
    if set_tags is not None:
        tag_set = set(set_tags)
    else:
        tag_set = set(tags)
    tag_set.update(add_tags)
    tag_set.difference_update(remove_tags)
    return sorted(tag_set)

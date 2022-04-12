import re
from typing import Optional

import pytest

from ldb.main import main
from ldb.utils import DATASET_PREFIX, ROOT

from .data import SIMPLE_QUERY_DATA
from .utils import index_fashion_mnist


def int_group1(pattern: str, string: str) -> Optional[int]:
    match = re.search(pattern, string)
    if match is None:
        return None
    return int(match.group(1))


@pytest.mark.parametrize(
    "args,data_objs,annots",
    SIMPLE_QUERY_DATA.values(),
    ids=SIMPLE_QUERY_DATA.keys(),
)
def test_cli_tag_root_dataset(
    args,
    data_objs,
    annots,  # pylint: disable=unused-argument
    ldb_instance,
    capsys,
):
    index_fashion_mnist(ldb_instance)
    ret = main(
        ["tag", f"{DATASET_PREFIX}{ROOT}", *args, "-a", "new-tag-1,new-tag-2"],
    )
    captured = capsys.readouterr().out
    data_obj_match = int_group1(r"\n  Data objects: +(\d+)\n", captured)
    updated_match = int_group1(r"\n  Num updated: +(\d+)\n", captured)
    assert ret == 0
    assert data_obj_match == data_objs
    assert updated_match == data_objs

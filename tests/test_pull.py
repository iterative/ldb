import pytest

from ldb.main import main
from ldb.path import WorkspacePath
from ldb.utils import DATASET_PREFIX, ROOT
from ldb.workspace import collection_dir_to_object

from .data import QUERY_DATA
from .utils import DATA_DIR, add_user_filter, index_fashion_mnist

ORIGINAL_COLLECTION = {
    "1e0759182b328fd22fcdb5e6beb54adf": None,
    "218f6c2347471a309163301f9110ee23": "5bd583e12fd78ccc9dc61a36debd985f",
    "232bab540dbfbd2fccae2e57e684663e": "ef8b9794e2e24d461477fc6b847e8540",
    "2c4a9d28cc2ce780d17bea08d45d33b3": "5ca184106560369a01db4fdfc3bbf5da",
    "2f3533f1e35349602fbfaf0ec9b3ef3f": None,
    "31ed21a2633c6802e756dd06220b0b82": "d3735bac9773c15e1bf3aa891f05c9da",
    "333ddb9f27aeaf4050b9c5c5e46005ff": None,
    "399146164375493f916025b04d00709c": "97dde24d0e61ac83f051cd748e16f5dc",
    "3c679fd1b8537dc7da1272a085e388e6": None,
    "47149106168f7d88fcea9e168608f129": "062133135568b9e077d15703593fb0e6",
    "5dc54ca1daee20ed68568e9b15c949fe": None,
    "65383bee429980b89febc3f9b3349379": "5bd583e12fd78ccc9dc61a36debd985f",
    "66e0373a2a989870fbc2c7791d8e6490": "ef8b9794e2e24d461477fc6b847e8540",
    "751111c36f27e3668b9b043987c18386": "d3735bac9773c15e1bf3aa891f05c9da",
    "93e2a847c9341054107d8e93a259a9c8": None,
    "95789bb1ac140460cefc97a6e66a9ee8": "268daa854dde9f160c2b2ffe1d2ed74b",
    "963ba51db79d1749d622f79270423093": None,
    "982814b9116dce7882dfc31636c3ff7a": "97dde24d0e61ac83f051cd748e16f5dc",
    "a2430513e897d5abcf62a55b8df81355": "8d68100832b01b8b8470a14b467d2f63",
    "ab15b45e6a809aeeefdd3e9cbbe2d27e": None,
    "b056f7ef766d698aee2542150f1add72": "fca0c632ff06eb77af414522df2c0c9e",
    "b5fba326c8247d9e62aa17a109146c02": "5a86b5dbd8161f6fb6ec7c3b6a75ec5c",
    "ccb47dff4477d8492326a45423b0faca": "5ca184106560369a01db4fdfc3bbf5da",
    "d0346148afcebd9cfccc809359baa4d8": "e6296862bef46d99f0b1f26c8e84dc22",
    "d830d9f128e04678499e1fc52e935c4a": "e6296862bef46d99f0b1f26c8e84dc22",
    "def3cbcb30f3254a2a220e51ddf45375": "ef8b9794e2e24d461477fc6b847e8540",
    "e1c3ef93e4e1cf108fa2a4c9d6e03af2": None,
    "e299594dc1f79f8e69c6d79a42699822": "3ee7b8de6da6d440c43f7afecaf590ef",
    "e38e92eaf034d00039750a8b1001ff22": "46fa5381b9cd9433f03670ca9d7828dc",
    "ebbc6c0cebb66738942ee56513f9ee2f": "268daa854dde9f160c2b2ffe1d2ed74b",
    "ec27abb9c90f3f86f75eda4323a17ade": "d3735bac9773c15e1bf3aa891f05c9da",
    "eea5f6cb86d2c3bc9928c808c9229dda": "16783d22c2764315a909d38bc6e5ddee",
}
UPDATED_COLLECTION = {
    "1e0759182b328fd22fcdb5e6beb54adf": "9daeeb2b07054600c12e97149136147c",
    "218f6c2347471a309163301f9110ee23": "9daeeb2b07054600c12e97149136147c",
    "232bab540dbfbd2fccae2e57e684663e": "bb011d4ab3e9cfc17c82f27f4ab8cdd6",
    "2c4a9d28cc2ce780d17bea08d45d33b3": "74e717c7fde48dc0de63cba8284a716d",
    "2f3533f1e35349602fbfaf0ec9b3ef3f": "437e068b3aa4b16b080b01fceaeee09b",
    "31ed21a2633c6802e756dd06220b0b82": "99b649a813e82a9d55344f274a9f106d",
    "333ddb9f27aeaf4050b9c5c5e46005ff": "74e717c7fde48dc0de63cba8284a716d",
    "399146164375493f916025b04d00709c": "9f0583102b39d7a98f49d3a8935c6ee1",
    "3c679fd1b8537dc7da1272a085e388e6": "87c29a25dc81ee78c511c50b158b8c4d",
    "47149106168f7d88fcea9e168608f129": "9f0583102b39d7a98f49d3a8935c6ee1",
    "5dc54ca1daee20ed68568e9b15c949fe": "437e068b3aa4b16b080b01fceaeee09b",
    "65383bee429980b89febc3f9b3349379": "9daeeb2b07054600c12e97149136147c",
    "66e0373a2a989870fbc2c7791d8e6490": "bb011d4ab3e9cfc17c82f27f4ab8cdd6",
    "751111c36f27e3668b9b043987c18386": "99b649a813e82a9d55344f274a9f106d",
    "93e2a847c9341054107d8e93a259a9c8": "9f0583102b39d7a98f49d3a8935c6ee1",
    "95789bb1ac140460cefc97a6e66a9ee8": "0279eae0bcea653151c2569335eeeb6d",
    "963ba51db79d1749d622f79270423093": "9f0583102b39d7a98f49d3a8935c6ee1",
    "982814b9116dce7882dfc31636c3ff7a": "9f0583102b39d7a98f49d3a8935c6ee1",
    "a2430513e897d5abcf62a55b8df81355": "0279eae0bcea653151c2569335eeeb6d",
    "ab15b45e6a809aeeefdd3e9cbbe2d27e": "87c29a25dc81ee78c511c50b158b8c4d",
    "b056f7ef766d698aee2542150f1add72": "437e068b3aa4b16b080b01fceaeee09b",
    "b5fba326c8247d9e62aa17a109146c02": "b9df6d28cfc8b8f31c659558cb9909bb",
    "ccb47dff4477d8492326a45423b0faca": "74e717c7fde48dc0de63cba8284a716d",
    "d0346148afcebd9cfccc809359baa4d8": "b9df6d28cfc8b8f31c659558cb9909bb",
    "d830d9f128e04678499e1fc52e935c4a": "b9df6d28cfc8b8f31c659558cb9909bb",
    "def3cbcb30f3254a2a220e51ddf45375": "bb011d4ab3e9cfc17c82f27f4ab8cdd6",
    "e1c3ef93e4e1cf108fa2a4c9d6e03af2": "d773d40dbabda031a781180265eef357",
    "e299594dc1f79f8e69c6d79a42699822": "d773d40dbabda031a781180265eef357",
    "e38e92eaf034d00039750a8b1001ff22": "d773d40dbabda031a781180265eef357",
    "ebbc6c0cebb66738942ee56513f9ee2f": "0279eae0bcea653151c2569335eeeb6d",
    "ec27abb9c90f3f86f75eda4323a17ade": "99b649a813e82a9d55344f274a9f106d",
    "eea5f6cb86d2c3bc9928c808c9229dda": "87c29a25dc81ee78c511c50b158b8c4d",
}
DATA_OBJ_HASHES = [
    "1e0759182b328fd22fcdb5e6beb54adf",
    "218f6c2347471a309163301f9110ee23",
    "a2430513e897d5abcf62a55b8df81355",
    "e299594dc1f79f8e69c6d79a42699822",
    "ec27abb9c90f3f86f75eda4323a17ade",
]
ANNOT_VERSION_HASHES = [
    ("9daeeb2b07054600c12e97149136147c",),
    ("9daeeb2b07054600c12e97149136147c", "5bd583e12fd78ccc9dc61a36debd985f"),
    (
        "0279eae0bcea653151c2569335eeeb6d",
        "268daa854dde9f160c2b2ffe1d2ed74b",
        "8d68100832b01b8b8470a14b467d2f63",
    ),
    (
        "d773d40dbabda031a781180265eef357",
        "46fa5381b9cd9433f03670ca9d7828dc",
        "3ee7b8de6da6d440c43f7afecaf590ef",
    ),
    (
        "99b649a813e82a9d55344f274a9f106d",
        "d3735bac9773c15e1bf3aa891f05c9da",
    ),
]


@pytest.mark.parametrize(
    "args,data_objs,annots",
    QUERY_DATA.values(),
    ids=QUERY_DATA.keys(),
)
def test_pull_query_data(
    args,
    data_objs,
    annots,  # pylint: disable=unused-argument
    ldb_instance,
    workspace_path,
):
    add_user_filter(ldb_instance)
    index_fashion_mnist(ldb_instance)
    main(["add", f"{DATASET_PREFIX}{ROOT}"])
    main(["index", str(DATA_DIR / "predictions")])
    ret = main(["pull", *args])
    ws_collection = collection_dir_to_object(
        workspace_path / WorkspacePath.COLLECTION,
    )
    original_counts = 0
    updated_counts = 0
    for data_obj_hash, annot_hash in ws_collection.items():
        if annot_hash == UPDATED_COLLECTION[data_obj_hash]:
            updated_counts += 1
        elif annot_hash == ORIGINAL_COLLECTION[data_obj_hash]:
            original_counts += 1
    assert ret == 0
    assert updated_counts == data_objs
    assert original_counts == 32 - data_objs


@pytest.mark.parametrize(
    "args,annot_indices",
    [
        (["v1"], [0, 0, 0, 0, 0]),
        (["v2"], [0, 1, 1, 1, 1]),
        (["v3"], [0, 0, 2, 2, 0]),
        ([], [0, 1, 2, 2, 1]),
        (["--query", "contains([`0`, `2`], label)", "v2"], [0, 0, 0, 1, 1]),
        (["--query", "contains([`0`, `2`], label)"], [0, 0, 0, 2, 1]),
    ],
)
def test_pull_specific_version(
    args,
    annot_indices,
    ldb_instance,
    workspace_path,
):
    main(["index", str(DATA_DIR / "predictions")])
    main(["add", *[f"id:{h}" for h in DATA_OBJ_HASHES]])
    main(["index", str(DATA_DIR / "fashion-mnist/original")])
    main(["index", str(DATA_DIR / "fashion-mnist/updates")])
    ret = main(["pull", *args])
    ws_collection = collection_dir_to_object(
        workspace_path / WorkspacePath.COLLECTION,
    )
    expected = dict(
        zip(
            DATA_OBJ_HASHES,
            [a[i] for a, i in zip(ANNOT_VERSION_HASHES, annot_indices)],
        ),
    )
    assert ret == 0
    assert ws_collection == expected

import sys

from .utils import SCRIPTS_DIR

REVERSE_SCRIPT = str(SCRIPTS_DIR / "reverse")
SORT_Q1 = ["--pipe", sys.executable, f"{REVERSE_SCRIPT}"]
FILE_Q1 = ["--file", "fs.size > `400`"]
ANNOT_Q1 = [
    "--query",
    "@ == `null` || inference.label != `null`",
]
BASIC_QUERIES = [*FILE_Q1, *ANNOT_Q1]
# args,data_objs,annots
PIPE_QUERY_DATA = {
    "pipe": (["--pipe=reverse", "--limit", "12", "--query", "@"], 11, 11),
}
SIMPLE_QUERY_DATA = {
    "no-args": ([], 32, 23),
    "sample1": (["--sample", "1.0"], 32, 23),
    "sample0": (["--sample", "0.0"], 0, 0),
    "shuffle": (["--shuffle"], 32, 23),
    "limit12": (["--limit", "12"], 12, 7),
    "limit-sort": (["--limit", "12", *SORT_Q1, "--query", "@"], 7, 7),
    "sort-limit": ([*SORT_Q1, "--limit", "12", "--query", "@"], 11, 11),
    "file@": (["--file", "@"], 32, 23),
    "query@": (["--query", "@"], 23, 23),
    "fsize": (["--file", "fs.size > `400`"], 26, 20),
    "annot1": (ANNOT_Q1, 13, 4),
    "basic-queries": (BASIC_QUERIES, 9, 3),
    "limit-basic": (["--limit", "22", *BASIC_QUERIES], 7, 2),
    "basic-fsize": ([*BASIC_QUERIES, "--file", "fs.size < `600`"], 7, 3),
    "tag-notag": (["--tag", "a,c,e", "--no-tag", "b,d"], 23, 14),
    "tag": (["--tag", "d"], 9, 9),
    "path": (["--path", r"/\d*3\d*\.png"], 7, 5),
    "complex-pipeline": (
        [
            "--sample=1.0",
            "--limit=100",
            "--query=@ != `null`",
            '--file=type(fs.size) == `"number"`',
            "--file=fs.size > `100`",
            '--jquery=type(label) == `"number"`',
            "--sample=1.0",
            "--tag=b",
            "--query=not_null(inference.label)",
            '--query=type(inference.label) == `"number"`',
            "--no-tag=missing-tag",
            "--limit=10",
            "--query=inference.label <= label",
            *SORT_Q1,
            "--limit=5",
            "--shuffle",
        ],
        3,
        3,
    ),
}
QUERY_DATA = {
    **SIMPLE_QUERY_DATA,
    **PIPE_QUERY_DATA,
}
LABEL_STUDIO_ANNOTATIONS = {
    "25": {
        "data": {
            "label": 4,
            "image": "00025.png",
            "data-object-info": {
                "md5": "982814b9116dce7882dfc31636c3ff7a",
                "path_key": "data.image",
            },
        }
    },
    "47": {
        "data": {
            "label": 7,
            "image": "00047.png",
            "data-object-info": {
                "md5": "ebbc6c0cebb66738942ee56513f9ee2f",
                "path_key": "data.image",
            },
        }
    },
    "70": {
        "data": {
            "label": 1,
            "image": "00070.png",
            "data-object-info": {
                "md5": "b056f7ef766d698aee2542150f1add72",
                "path_key": "data.image",
            },
        }
    },
}

import sys

from .utils import SORT_DIR

REVERSE_SCRIPT = str(SORT_DIR / "reverse")
SORT_Q1 = ["--sort", sys.executable, f"{REVERSE_SCRIPT}"]
FILE_Q1 = ["--file", "fs.size > `400`"]
ANNOT_Q1 = ["--query", "@ == `null` || inference.label != `null`"]
BASIC_QUERIES = [*FILE_Q1, *ANNOT_Q1]
# args,data_objs,annots
QUERY_DATA = [
    ([], 32, 23),
    (["--sample", "1.0"], 32, 23),
    (["--sample", "0.0"], 0, 0),
    (["--limit", "12"], 12, 7),
    (["--limit", "12", *SORT_Q1, "--query", "@"], 7, 7),
    ([*SORT_Q1, "--limit", "12", "--query", "@"], 11, 11),
    (["--sort=reverse", "--limit", "12", "--query", "@"], 11, 11),
    (["--file", "@"], 32, 23),
    (["--query", "@"], 23, 23),
    (["--file", "fs.size > `400`"], 26, 20),
    (ANNOT_Q1, 13, 4),
    (BASIC_QUERIES, 9, 3),
    (["--limit", "22", *BASIC_QUERIES], 7, 2),
    ([*BASIC_QUERIES, "--file", "fs.size < `600`"], 7, 3),
    (
        [
            "--sample=1.0",
            "--limit=100",
            "--query=@ != `null`",
            '--file=type(fs.size) == `"number"`',
            "--file=fs.size > `100`",
            '--query=type(label) == `"number"`',
            "--sample=1.0",
            "--query=inference.label != `null`",
            '--query=type(inference.label) == `"number"`',
            "--limit=10",
            "--query=inference.label <= label",
            *SORT_Q1,
            "--limit=5",
        ],
        3,
        3,
    ),
]

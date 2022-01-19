from .utils import SORT_DIR

REVERSE_SCRIPT = str(SORT_DIR / "reverse")

BASIC_FILE_QUERY = ["--file", "fs.size > `400`"]
BASIC_ANNOT_QUERY = ["--query", "@ == `null` || inference.label != `null`"]
BASIC_QUERIES = [*BASIC_FILE_QUERY, *BASIC_ANNOT_QUERY]
# args,data_objs,annots
QUERY_DATA = [
    ([], 32, 23),
    (["--sample", "1.0"], 32, 23),
    (["--sample", "0.0"], 0, 0),
    (["--limit", "12"], 12, 7),
    (["--limit", "12", "--sort", f"{REVERSE_SCRIPT}", "--query", "@"], 7, 7),
    (["--sort", f"{REVERSE_SCRIPT}", "--limit", "12", "--query", "@"], 11, 11),
    (["--file", "@"], 32, 23),
    (["--query", "@"], 23, 23),
    (["--file", "fs.size > `400`"], 26, 20),
    (BASIC_ANNOT_QUERY, 13, 4),
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
            f"--sort={REVERSE_SCRIPT}",
            "--limit=5",
        ],
        3,
        3,
    ),
]

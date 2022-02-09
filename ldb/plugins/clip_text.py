import json
import sys

from .clip_utils import text_similarity
from .utils import sort_by_iterable


def main() -> None:
    if len(sys.argv) < 2:
        print(
            "usage: clip-text <text> [<model_name>]",
            file=sys.stderr,
        )
        sys.exit(1)
    text = sys.argv[1]
    model_name = sys.argv[2] if len(sys.argv) > 2 else None
    data = json.loads(sys.stdin.read())
    if not data:
        return

    file_paths = [d[1] for d in data]
    similarity = text_similarity(text, file_paths, model_name)
    for data_object_hash, _, _ in sort_by_iterable(data, similarity):
        print(data_object_hash, flush=True)


if __name__ == "__main__":
    main()

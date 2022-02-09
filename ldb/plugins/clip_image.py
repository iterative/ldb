import json
import sys

from .clip_utils import image_similarity
from .utils import sort_by_iterable


def main() -> None:
    if len(sys.argv) < 2:
        print(
            "usage: clip-image <image_file_path> [<model_name>]",
            file=sys.stderr,
        )
        sys.exit(1)

    image_filepath = sys.argv[1]
    model_name = sys.argv[2] if len(sys.argv) > 2 else None
    data = json.loads(sys.stdin.read())
    file_paths = [d[1] for d in data]
    similarity = image_similarity(image_filepath, file_paths, model_name)
    for data_object_hash, _, _ in sort_by_iterable(data, similarity):
        print(data_object_hash, flush=True)


if __name__ == "__main__":
    main()

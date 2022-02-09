import json
import sys

from .resnet_utils import image_layer_similarity
from .utils import sort_by_iterable


def main() -> None:
    if len(sys.argv) < 2:
        print(
            "usage: resnet-image <image_file_path> [<model_number>]",
            file=sys.stderr,
        )
        sys.exit(1)

    image_filepath = sys.argv[1]
    model_name = "18"
    layer_index = None
    if len(sys.argv) > 2:
        model_name = sys.argv[2]
        if len(sys.argv) > 3:
            layer_index = int(sys.argv[3])
            if not 1 <= layer_index <= 4:
                raise ValueError(
                    "Layer number must be in the range (1, 4) inclusive",
                )
            layer_index -= 1
    data = json.loads(sys.stdin.read())
    file_paths = [d[1] for d in data]
    similarity = image_layer_similarity(
        image_filepath,
        file_paths,
        model_name,
        layer_index,
    )
    for data_object_hash, _, _ in sort_by_iterable(data, similarity):
        print(data_object_hash, flush=True)


if __name__ == "__main__":
    main()

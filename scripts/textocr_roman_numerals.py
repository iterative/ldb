#!/usr/bin/env python3
import json
import os
import re
import sys
from typing import Sequence

from PIL import Image


def main(argv: Sequence[str] = ()) -> None:
    source_dir = argv[0]
    dest_dir = argv[1]
    file_names = set(os.listdir(source_dir))
    pairs = {}
    for entry in file_names:
        if not entry.endswith(".json"):
            annot_path = os.path.splitext(entry)[0] + ".json"
            if annot_path in file_names:
                pairs[annot_path] = entry

    for annot_name, data_obj_name in pairs.items():
        create_roman_numeral_crops(
            os.path.join(source_dir, data_obj_name),
            os.path.join(source_dir, annot_name),
            dest_dir,
        )


def create_roman_numeral_crops(
    data_obj_path: str,
    annot_path: str,
    dest_dir: str,
) -> None:
    with open(annot_path, encoding="utf-8") as source_file:
        raw_annot = source_file.read()
    annot = json.loads(raw_annot)
    sub_annots = [
        a for a in annot["anns"] if is_simple_roman_numeral(a["utf8_string"])
    ]
    if sub_annots:
        data_obj_name = os.path.basename(data_obj_path)
        dest_base, ext = os.path.splitext(
            os.path.join(dest_dir, data_obj_name),
        )
        source_img = annot["img"]
        img = Image.open(data_obj_path)
        for i, sub_annot in enumerate(sub_annots, 1):
            new_path_base = f"{dest_base}-{i:03}"
            new_data_obj_path = f"{new_path_base}{ext}"
            new_annot_path = f"{new_path_base}.json"
            new_img = crop_bbox(img, sub_annot["bbox"])
            new_annot = {
                "source_img": source_img,
                "annotation": sub_annot,
            }
            new_raw_annot = json.dumps(new_annot, indent=2)
            with open(new_annot_path, "x", encoding="utf-8") as dest_file:
                dest_file.write(new_raw_annot)
            new_img.save(new_data_obj_path)


def is_simple_roman_numeral(text: str) -> bool:
    return bool(re.search("(?i)^(V?I{0,3}|IV|IX|X)$", text))


def crop_bbox(image: Image.Image, bbox: Sequence[float]) -> Image.Image:
    x1, y1, x2, y2 = bbox
    x2 += x1
    y2 += y1
    coordinates = x1, y1, x2, y2
    return image.crop(coordinates)  # type: ignore[arg-type]


if __name__ == "__main__":
    main(json.loads(sys.stdin.read()))

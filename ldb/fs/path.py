"""
Originally from dvc.fs.path.

Copied version:
  https://github.com/iterative/dvc/blob/e80e725fe45f74141e273dccf47f6a61b942b412/dvc/fs/path.py
Current version:
  https://github.com/iterative/dvc/blob/main/dvc/fs/path.py
"""
import ntpath
import posixpath
from typing import List, Tuple


class Path:
    def __init__(self, sep: str) -> None:
        if sep == posixpath.sep:
            self.flavour = posixpath
        elif sep == ntpath.sep:
            self.flavour = ntpath  # type: ignore[misc]
        else:
            raise ValueError(f"unsupported separator '{sep}'")

    def join(self, *parts: str) -> str:
        return self.flavour.join(*parts)

    def parts(self, path: str) -> Tuple[str, ...]:
        drive, path = self.flavour.splitdrive(path)

        ret: List[str] = []
        while True:
            path, part = self.flavour.split(path)

            if part:
                ret.append(part)
                continue

            if path:
                ret.append(path)

            break

        ret.reverse()

        if drive:
            ret = [drive] + ret

        return tuple(ret)

    def parent(self, path: str) -> str:
        return self.flavour.dirname(path)

    def parents(self, path: str) -> Tuple[str, ...]:
        parts = self.parts(path)
        return tuple(
            self.join(*parts[:length])
            for length in range(len(parts) - 1, 0, -1)
        )

    def name(self, path: str) -> str:
        return self.parts(path)[-1]

    def suffix(self, path: str) -> str:
        name = self.name(path)
        _, dot, suffix = name.partition(".")
        return dot + suffix

    def with_name(self, path: str, name: str) -> str:
        parts = list(self.parts(path))
        parts[-1] = name
        return self.join(*parts)

    def with_suffix(self, path: str, suffix: str) -> str:
        parts = list(self.parts(path))
        real_path, _, _ = parts[-1].partition(".")
        parts[-1] = real_path + suffix
        return self.join(*parts)

    def isin(self, left: str, right: str) -> bool:
        left_parts = self.parts(left)
        right_parts = self.parts(right)
        left_len = len(left_parts)
        right_len = len(right_parts)
        return left_len > right_len and left_parts[:right_len] == right_parts

    def isin_or_eq(self, left: str, right: str) -> bool:
        return left == right or self.isin(left, right)

    def overlaps(self, left: str, right: str) -> bool:
        # pylint: disable=arguments-out-of-order
        return self.isin_or_eq(left, right) or self.isin(right, left)

    def relpath(self, path: str, start: str) -> str:
        assert start
        return self.flavour.relpath(path, start=start)

    def relparts(self, path: str, base: str) -> Tuple[str, ...]:
        return self.parts(self.relpath(path, base))

    def as_posix(self, path: str) -> str:
        return path.replace(self.flavour.sep, posixpath.sep)

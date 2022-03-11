__all__ = ["posix_path"]

from .path import Path

posix_path = Path("/")

del Path

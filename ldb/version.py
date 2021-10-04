"""
Version info management
"""
__all__ = ["__version__"]

import os

with open(
    os.path.join(os.path.dirname(__file__), "VERSION"),
    encoding="utf-8",
) as version_file:
    __version__ = version_file.read().strip()
del os, version_file

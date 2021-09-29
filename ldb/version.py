__all__ = ["__version__"]

import os

with open(os.path.join(os.path.dirname(__file__), "VERSION")) as version_file:
    __version__ = version_file.read().strip()
del os, version_file

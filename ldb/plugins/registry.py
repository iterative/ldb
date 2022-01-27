import shlex
from typing import NoReturn


def dependency_exc(exc: Exception, extra: str) -> NoReturn:
    package = shlex.quote(f"ldb-alpha[{extra}]")
    raise ImportError(
        "Missing dependency. To install all dependencies run:\n\n"
        f"\tpip install {package}\n",
    ) from exc


def validate_clip() -> None:
    try:
        import clip  # noqa: F401
    except ModuleNotFoundError as exc:
        dependency_exc(exc, "clip-plugin")


def clip_text() -> None:
    validate_clip()

    from .clip_text import main

    main()


def clip_image() -> None:
    validate_clip()

    from .clip_image import main

    main()

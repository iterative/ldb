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
        import clip  # pylint: disable=import-outside-toplevel,unused-import # noqa: E501, F401
    except ModuleNotFoundError as exc:
        dependency_exc(exc, "clip-plugin")


def validate_resnet() -> None:
    try:
        import torch  # pylint: disable=import-outside-toplevel,unused-import # noqa: E501, F401
        import torchvision  # pylint: disable=import-outside-toplevel,unused-import # noqa: E501, F401
    except ModuleNotFoundError as exc:
        dependency_exc(exc, "clip-plugin")


def clip_text() -> None:
    validate_clip()

    from .clip_text import main  # pylint: disable=import-outside-toplevel

    main()


def clip_image() -> None:
    validate_clip()

    from .clip_image import main  # pylint: disable=import-outside-toplevel

    main()


def resnet_image() -> None:
    validate_resnet()

    from .resnet_image import main  # pylint: disable=import-outside-toplevel

    main()

import sys
import warnings
from typing import Callable, Optional, TextIO, Type, Union
from warnings import WarningMessage

FormatFunc = Callable[[WarningMessage], str]


class WarningHandler:
    _formatwarnmsg_impl: FormatFunc = warnings._formatwarnmsg_impl  # type: ignore[attr-defined] # pylint: disable=protected-access # noqa: E501

    @classmethod
    def showwarning(
        cls,
        message: Union[Warning, str],
        category: Type[Warning],
        filename: str,
        lineno: int,
        file: Optional[TextIO] = None,
        line: Optional[str] = None,
    ) -> None:
        """
        Write a warning to a file, based on warnings.showwarning.
        """
        msg = WarningMessage(message, category, filename, lineno, file, line)
        cls._showwarnmsg_impl(msg)

    @classmethod
    def formatwarning(
        cls,
        message: Union[Warning, str],
        category: Type[Warning],
        filename: str,
        lineno: int,
        line: Optional[str] = None,
    ) -> str:
        """
        Format a warning, based on warnings.formatwarning.
        """
        msg = WarningMessage(message, category, filename, lineno, None, line)
        return cls._formatwarnmsg_impl(msg)

    @classmethod
    def _showwarnmsg_impl(cls, msg: WarningMessage) -> None:
        """
        Print a WarningMessage object to a file.

        Based on warnings._showwarnmsg_impl.
        """
        file = msg.file
        if file is None:
            file = sys.stderr
            if file is None:
                # sys.stderr is None when run with pythonw.exe:
                # warnings get lost
                return  # type: ignore[unreachable]
        text = cls._formatwarnmsg_impl(msg)
        try:
            file.write(text)
        except OSError:
            # the file (probably stderr) is invalid - this warning gets lost.
            pass


def simple_formatwarnmsg(msg: WarningMessage) -> str:
    return f"{msg.category.__name__}: {msg.message}\n"


class SimpleWarningHandler(WarningHandler):
    _formatwarnmsg_impl = simple_formatwarnmsg

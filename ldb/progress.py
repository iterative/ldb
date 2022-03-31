from contextlib import contextmanager
from typing import Any, Generator

from rich.progress import BarColumn, Progress, TextColumn, TimeRemainingColumn


@contextmanager
def get_progressbar(
    *args: Any,
    **kwargs: Any,
) -> Generator[Progress, None, None]:
    with Progress(
        TextColumn(
            "[bold blue]{task.completed}/{task.total}",
            justify="right",
        ),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        TimeRemainingColumn(),
        *args,
        **kwargs,
    ) as progress:
        yield progress

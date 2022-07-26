from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from ldb.cli_utils import add_instantiate_arguments
from ldb.core import get_ldb_instance
from ldb.instantiate import instantiate

if TYPE_CHECKING:
    from argparse import _SubParsersAction

    from ldb.cli import ArgumentParserT


def instantiate_command(options: Namespace) -> None:
    ldb_dir = get_ldb_instance()
    result = instantiate(
        ldb_dir,
        Path(options.target_dir),
        options.paths,
        options.query_args,
        fmt=options.format,
        force=options.force,
        apply=options.apply,
        warn=False,
        params=dict(options.params),
    )
    print(
        "Copied dataset to workspace.\n"
        f"  Data objects: {result.num_data_objects:9d}\n"
        f"  Annotations:  {result.num_annotations:9d}",
    )


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParserT]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "instantiate",
        parents=parents,
        help="Instantiate the current workspace dataset",
    )
    add_instantiate_arguments(parser)
    parser.set_defaults(func=instantiate_command)

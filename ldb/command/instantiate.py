from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from ldb.cli_utils import add_data_format_arguments, add_target_dir_argument
from ldb.core import get_ldb_instance
from ldb.data_formats import INSTANTIATE_FORMATS, Format
from ldb.instantiate import instantiate
from ldb.utils import format_dataset_identifier
from ldb.workspace import load_workspace_dataset

if TYPE_CHECKING:
    from argparse import _SubParsersAction


def instantiate_command(options: Namespace) -> None:
    ldb_dir = get_ldb_instance()
    workspace_path = Path(".")
    workspace_ds = load_workspace_dataset(workspace_path)
    ds_ident = format_dataset_identifier(workspace_ds.dataset_name)
    print(f"Instantiating working dataset {ds_ident}...\n")
    result = instantiate(
        ldb_dir,
        workspace_path,
        Path(options.target_dir),
        fmt=options.format,
        force=options.force,
        apply=options.apply,
    )
    print(
        "Copied dataset to workspace.\n"
        f"  Data objects: {result.num_data_objects:9d}\n"
        f"  Annotations:  {result.num_annotations:9d}",
    )


def add_parser(
    subparsers: "_SubParsersAction[ArgumentParser]",
    parents: Iterable[ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "instantiate",
        parents=parents,
        help="Instantiate the current workspace dataset",
    )
    add_data_format_arguments(
        parser,
        default=Format.BARE,
        formats=INSTANTIATE_FORMATS,
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Remove existing workspace contents",
    )
    add_target_dir_argument(parser)
    parser.add_argument(
        "--apply",
        nargs="+",
        metavar="<exec>",
        default=None,
        dest="apply",
        help="Executable to apply to data objects and annotations",
    )
    parser.set_defaults(func=instantiate_command)

from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from ldb.cli_utils import add_instantiate_arguments
from ldb.core import get_ldb_instance
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
        options.paths,
        options.query_args,
        fmt=options.format,
        force=options.force,
        apply=options.apply,
        warn=False,
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
    add_instantiate_arguments(parser)
    parser.set_defaults(func=instantiate_command)

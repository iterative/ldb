import argparse
from pathlib import Path
from typing import Iterable

from ldb.core import get_ldb_instance
from ldb.instantiate import instantiate
from ldb.utils import format_dataset_identifier
from ldb.workspace import load_workspace_dataset


def instantiate_command(options):
    ldb_dir = get_ldb_instance()
    workspace_path = Path(".")
    workspace_ds = load_workspace_dataset(workspace_path)
    ds_ident = format_dataset_identifier(workspace_ds.dataset_name)
    print(f"Instantiating working dataset {ds_ident}...\n")
    num_data_objects, num_annotations = instantiate(
        ldb_dir,
        workspace_path,
        force=options.force,
    )
    print(
        "Copied dataset to workspace.\n"
        f"  Data objects: {num_data_objects:9d}\n"
        f"  Annotations:  {num_annotations:9d}",
    )


def add_parser(
    subparsers: argparse._SubParsersAction,
    parents: Iterable[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "instantiate",
        parents=parents,
        help="Instantiate the current workspace dataset",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Remove existing workspace contents",
    )
    parser.set_defaults(func=instantiate_command)
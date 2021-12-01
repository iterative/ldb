import argparse

import shtab


def add_data_object_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--query",
        metavar="<query>",
        dest="annotation_query",
        help="JMESPath query applied to annotations",
    )
    parser.add_argument(
        "--file",
        metavar="<query>",
        dest="file_query",
        help="JMESPath query applied to file attributes",
    )
    parser.add_argument(  # type: ignore[attr-defined]
        "paths",
        metavar="<path>",
        nargs="*",
        help="Storage location, data object identifier, or dataset",
    ).complete = shtab.FILE

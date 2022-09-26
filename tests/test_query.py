import io
import json

import pytest

from ldb.main import main

from .utils import QUERY_TEST_JSON_DATA


@pytest.mark.parametrize(
    "indent_args,expected",
    [
        ([], '{\n  "width": 1024,\n  "height": 768\n}'),
        (
            ["--indent=2"],
            '{\n  "width": 1024,\n  "height": 768\n}',
        ),
        (["--indent=none"], '{"width": 1024, "height": 768}'),
        (
            ["--indent=\t"],
            '{\n\t"width": 1024,\n\t"height": 768\n}',
        ),
    ],
    ids=["no-arg", "2", "none", "tab"],
)
def test_cli_query_json_indent(
    indent_args,
    expected,
    query_test_json,
    fashion_mnist_session,
    capsys,
):
    main(
        [
            "query",
            "--show=details",
            *indent_args,
            query_test_json,
        ],
    )
    out_lines, err = capsys.readouterr()
    assert err == ""
    assert expected in out_lines


@pytest.mark.parametrize(
    "query_show_args,expected",
    [
        (
            ["--query=subject == 'cat'"],
            json.dumps(QUERY_TEST_JSON_DATA[0], indent=2),
        ),
        (["--query=subject == 'cat'", "--show=details.width"], "1024"),
        (["--query=format == 'jpg'", "--show=details.width"], "512"),
        (
            [
                "--query=subject == 'dog'",
                "--query=format == 'jpg'",
                "--show=details.width",
            ],
            "512",
        ),
        (
            [
                "--query=subject == 'cat'",
                "--show=details.width",
                "--show=details.height",
            ],
            "1024\n768",
        ),
        (
            [
                "--jquery=list_values[1] == `2`",
                "--jshow=details.width",
                "--jshow=details.height",
            ],
            "1024\n768",
        ),
        (
            ["--query=type == 'image'", "--show=subject", "--show=format"],
            '"cat"\n"png"\n"dog"\n"jpg"',
        ),
    ],
    ids=[
        "cat full",
        "cat",
        "jpg",
        "dog and jpg",
        "cat w/h",
        "list_values w/h",
        "both subject format",
    ],
)
def test_cli_query_json_multi_filter_and_show(
    query_show_args,
    expected,
    query_test_json,
    query_test_json2,
    fashion_mnist_session,
    capsys,
):
    main(
        [
            "query",
            *query_show_args,
            query_test_json,
            query_test_json2,
        ],
    )
    out_lines, err = capsys.readouterr()
    assert err == ""
    assert expected in out_lines


@pytest.mark.parametrize(
    "slurp_show_args,expected",
    [
        (["--slurp"], json.dumps(QUERY_TEST_JSON_DATA, indent=2)),
        (["--slurp", "--show=[0].subject"], '"cat"'),
        (["--slurp", "--show=[1].subject"], '"dog"'),
    ],
    ids=["slurp full", "cat", "dog"],
)
def test_cli_query_json_slurp_and_show(
    slurp_show_args,
    expected,
    query_test_json,
    query_test_json2,
    fashion_mnist_session,
    capsys,
):
    main(
        [
            "query",
            *slurp_show_args,
            query_test_json,
            query_test_json2,
        ],
    )
    out_lines, err = capsys.readouterr()
    assert err == ""
    assert expected in out_lines


@pytest.mark.parametrize(
    "unslurp_query_show_args,expected",
    [
        (
            ["--unslurp", "--query=subject == 'cat'"],
            json.dumps(QUERY_TEST_JSON_DATA[0], indent=2),
        ),
        (
            ["--unslurp", "--query=subject == 'cat'", "--show=details.width"],
            "1024",
        ),
        (
            ["--unslurp", "--query=format == 'jpg'", "--show=details.width"],
            "512",
        ),
        (
            [
                "--unslurp",
                "--query=list_values[1] == `2`",
                "--show=details.width",
                "--show=details.height",
            ],
            "1024\n768",
        ),
        (
            [
                "--unslurp",
                "--query=type == 'image'",
                "--show=subject",
                "--show=format",
            ],
            '"cat"\n"png"\n"dog"\n"jpg"',
        ),
    ],
    ids=[
        "unslurp full cat",
        "unslurp cat",
        "unslurp dog",
        "unslurp list_values w/h",
        "unslurp both subject format",
    ],
)
def test_cli_query_json_unslurp_query_and_show(
    unslurp_query_show_args,
    expected,
    query_test_combined,
    fashion_mnist_session,
    capsys,
):
    main(
        [
            "query",
            *unslurp_query_show_args,
            query_test_combined,
        ],
    )
    out_lines, err = capsys.readouterr()
    assert err == ""
    assert expected in out_lines


def test_cli_query_json_unslurp_with_stdin(
    monkeypatch,
    capsys,
):
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps(QUERY_TEST_JSON_DATA)),
    )
    main(
        [
            "query",
            "--unslurp",
            "--query=subject == 'cat'",
        ],
    )
    out_lines, err = capsys.readouterr()
    assert err == ""
    assert json.dumps(QUERY_TEST_JSON_DATA[0], indent=2) in out_lines

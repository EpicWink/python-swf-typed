"""Test ``swf_typed.__main__`` (ie module CLI)."""

import sys
import json
import pathlib
import subprocess

import pytest

data_directory_path = pathlib.Path(__file__).parent / "data"


@pytest.mark.parametrize("use_stdin", [
    pytest.param(False, id="input_file"),
    pytest.param(True, id="stdin"),
])  # fmt: skip
@pytest.mark.parametrize("use_stdout", [
    pytest.param(False, id="output_file"),
    pytest.param(True, id="stdout"),
])  # fmt: skip
def test_build_state(use_stdin: bool, use_stdout: bool, tmp_path: pathlib.Path) -> None:
    """Test 'build-state' subcommand."""

    # Arrange: specify file paths
    history_file_path = data_directory_path / "execution-history.json"
    state_file_path = tmp_path / "execution-state.json"

    # Act: run CLI
    cp = subprocess.run(
        [
            sys.executable,
            "-m",
            "swf_typed",
            "build-state",
            "-" if use_stdin else str(history_file_path),
            "-" if use_stdout else str(state_file_path),
        ],
        check=True,
        input=history_file_path.read_bytes() if use_stdin else None,
        stdout=subprocess.PIPE if use_stdout else None,
    )

    # Act: load result as JSON
    result_json_bytes = cp.stdout if use_stdout else state_file_path.read_bytes()
    result = json.loads(result_json_bytes.decode(encoding="utf-8"))

    # Assert: check result
    expected = json.loads(
        (data_directory_path / "execution-state.json")
        .read_bytes()
        .decode(encoding="utf-8"),
    )
    assert result == expected


@pytest.mark.parametrize("use_stdin", [
    pytest.param(False, id="input_file"),
    pytest.param(True, id="stdin"),
])  # fmt: skip
def test_format_state(use_stdin: bool, tmp_path: pathlib.Path) -> None:
    """Test 'format-state' subcommand."""
    state_file_path = data_directory_path / "execution-state.json"
    cp = subprocess.run(
        [
            sys.executable,
            "-m",
            "swf_typed",
            "format-state",
            "-" if use_stdin else str(state_file_path),
        ],
        check=True,
        input=state_file_path.read_bytes() if use_stdin else None,
        stdout=subprocess.PIPE,
    )
    assert cp.stdout == (data_directory_path / "execution-state.yml").read_bytes()

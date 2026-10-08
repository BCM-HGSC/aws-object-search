"""Test version functionality."""

import subprocess
import sys
from pathlib import Path

import pytest

from aws_object_search import __version__
from aws_object_search.entry import DOCS_URL

# Entry points are installed next to the interpreter running the tests.
SCRIPTS_DIR = Path(sys.executable).parent


def test_version_import():
    """Test that __version__ can be imported."""
    assert __version__ is not None
    assert isinstance(__version__, str)
    assert len(__version__) > 0


@pytest.mark.parametrize("command", ["aos-scan", "search-aws", "search.py"])
def test_entry_point_version(command):
    """Test that each entry point reports its name and version."""
    result = subprocess.run(
        [str(SCRIPTS_DIR / command), "--version"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert __version__ in result.stdout
    assert command in result.stdout


@pytest.mark.parametrize("command", ["aos-scan", "search-aws", "search.py"])
def test_entry_point_help_has_docs_url(command):
    """Test that each entry point's help links to the user documentation."""
    result = subprocess.run(
        [str(SCRIPTS_DIR / command), "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert DOCS_URL in result.stdout

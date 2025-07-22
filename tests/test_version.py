"""Test version functionality."""

import subprocess

from aws_object_search import __version__


def test_version_import():
    """Test that __version__ can be imported."""
    assert __version__ is not None
    assert isinstance(__version__, str)
    assert len(__version__) > 0


def test_aos_scan_version():
    """Test aos-scan --version command."""
    result = subprocess.run(
        ["bin/aos-scan", "--version"],
        capture_output=True,
        text=True,
        cwd="/Users/hale/Documents/dev/repo/aws-object-search/aos-repo"
    )
    assert result.returncode == 0
    assert __version__ in result.stdout
    assert "aos-scan" in result.stdout


def test_search_aws_version():
    """Test search-aws --version command."""
    result = subprocess.run(
        ["bin/search-aws", "--version"],
        capture_output=True,
        text=True,
        cwd="/Users/hale/Documents/dev/repo/aws-object-search/aos-repo"
    )
    assert result.returncode == 0
    assert __version__ in result.stdout
    assert "search-aws" in result.stdout


def test_search_py_version():
    """Test search.py --version command."""
    result = subprocess.run(
        ["bin/search.py", "--version"],
        capture_output=True,
        text=True,
        cwd="/Users/hale/Documents/dev/repo/aws-object-search/aos-repo"
    )
    assert result.returncode == 0
    assert __version__ in result.stdout
    assert "search.py" in result.stdout

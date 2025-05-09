"""
This file is used to configure pytest and add custom options or markers.
It allows for the inclusion of integration tests based on command-line options.
"""

from pathlib import Path

import pytest


def pytest_addoption(parser):
    """Pytest hook"""
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run tests marked as integration",
    )


def pytest_collection_modifyitems(config, items):
    """Pytest hook"""
    if config.getoption("--run-integration"):
        # If the option is set, do not skip integration tests
        return
    skip_integration = pytest.mark.skip(reason="Need --run-integration option to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)


@pytest.fixture
def resources_dir() -> Path:
    "Pytest fixture providing directory of test resources"
    return Path(__file__).parent / "resources"


@pytest.fixture
def simple_catalog_path(resources_dir) -> Path:
    "Directory containing a simple catalog in TSV"
    return resources_dir / "simple_catalog"

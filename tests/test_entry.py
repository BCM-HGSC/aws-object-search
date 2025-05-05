import argparse

import pytest

from aws_object_search.entry import aos_scan


@pytest.mark.integration
def test_aos_scan_smoke(tmp_path):
    """Smoke test using a specific bucket prefix and parent directory."""
    args = argparse.Namespace(
        bucket_prefix="test-bucket-prefix",
        parent_dir=tmp_path,
        log_level="ERROR",
    )
    aos_scan(args)

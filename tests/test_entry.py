import argparse

import pytest

from aws_object_search.entry import aos_scan


@pytest.mark.integration
def test_aos_scan_smoke(tmp_path):
    """Smoke test using a specific bucket prefix and output root directory."""
    args = argparse.Namespace(
        bucket_prefix="test-bucket-prefix",
        output_root=tmp_path,
        log_level="ERROR",
    )
    aos_scan(args)

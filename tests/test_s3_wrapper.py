import csv
import gzip
from datetime import UTC, datetime

import boto3
import pytest
from botocore.stub import Stubber

from aws_object_search.catalog import TSV_FIELDS
from aws_object_search.s3_wrapper import run_s3_object_scan

SCAN_PREFIX = "20250101-000000"


@pytest.fixture
def s3_client(monkeypatch):
    "S3 client with dummy credentials that never reaches AWS when stubbed."
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    return boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def test_run_s3_object_scan(tmp_path, s3_client):
    """Scan lists matching buckets, follows pagination, and writes TSV.gz."""
    modified = datetime(2025, 5, 3, 16, 48, 31, tzinfo=UTC)
    page_1 = [
        {
            "Key": "a/sample_R1_001.fastq.gz",
            "LastModified": modified,
            "ETag": '"0123abcd-2"',
            "Size": 1234,
            "StorageClass": "DEEP_ARCHIVE",
            "ChecksumAlgorithm": ["SHA256"],
            "ChecksumType": "COMPOSITE",
        }
    ]
    page_2 = [
        {
            "Key": "a/event.json",
            "LastModified": modified,
            "ETag": '"4567ef"',
            "Size": 56,
            "StorageClass": "STANDARD",
            "ChecksumAlgorithm": ["CRC64NVME"],
            "ChecksumType": "FULL_OBJECT",
        }
    ]
    with Stubber(s3_client) as stubber:
        stubber.add_response(
            "list_buckets",
            {"Buckets": [{"Name": "hgsc-x", "CreationDate": modified}]},
            {"Prefix": "hgsc-"},
        )
        stubber.add_response(
            "list_objects_v2",
            {"Contents": page_1, "IsTruncated": True, "NextContinuationToken": "t"},
            {"Bucket": "hgsc-x"},
        )
        stubber.add_response(
            "list_objects_v2",
            {"Contents": page_2, "IsTruncated": False},
            {"Bucket": "hgsc-x", "ContinuationToken": "t"},
        )
        run_s3_object_scan(tmp_path, "hgsc-", SCAN_PREFIX, s3_client)
        stubber.assert_no_pending_responses()

    tsv_path = tmp_path / f"{SCAN_PREFIX}-hgsc-x.tsv.gz"
    with gzip.open(tsv_path, "rt", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        assert reader.fieldnames == TSV_FIELDS
        rows = list(reader)
    assert rows == [
        {
            "last_modified": "2025-05-03T16:48:31+00:00",
            "size": "1234",
            "storage_class": "DEEP_ARCHIVE",
            "e_tag": "0123abcd-2",
            "checksum_algorithm": "SHA256",
            "checksum_type": "COMPOSITE",
            "key": "a/sample_R1_001.fastq.gz",
        },
        {
            "last_modified": "2025-05-03T16:48:31+00:00",
            "size": "56",
            "storage_class": "STANDARD",
            "e_tag": "4567ef",
            "checksum_algorithm": "CRC64NVME",
            "checksum_type": "FULL_OBJECT",
            "key": "a/event.json",
        },
    ]


def test_run_s3_object_scan_no_buckets(tmp_path, s3_client):
    """No matching buckets means no catalog files."""
    with Stubber(s3_client) as stubber:
        stubber.add_response("list_buckets", {"Buckets": []}, {"Prefix": "none-"})
        run_s3_object_scan(tmp_path, "none-", SCAN_PREFIX, s3_client)
    assert list(tmp_path.iterdir()) == []

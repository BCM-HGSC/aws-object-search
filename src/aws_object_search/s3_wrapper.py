from collections.abc import Generator
from logging import getLogger
from pathlib import Path

import boto3

from .catalog import S3ObjectCatalog

logger = getLogger(__name__)


def run_s3_object_scan(
    output_root: str | Path,
    bucket_prefix: str | None = None,
    tsv_file_prefix: str | None = None,
    s3_client=None,
) -> None:
    """
    Run a scan of S3 buckets and output their objects to TSV files.
    :param output_root: Output root directory for generated files
    :param bucket_prefix: Optional prefix to filter bucket names
    :param tsv_file_prefix: Optional value to use instead of timestamp in TSV file names
    :param s3_client: Optional Boto3 S3 client
    """
    if not isinstance(output_root, str | Path):
        raise TypeError(
            f"Output root directory {output_root!r} must be a string or Path or None"
        )
    if not isinstance(bucket_prefix, str | type(None)):
        raise TypeError("Bucket prefix must be a string or None")

    scanner = BucketScanner(s3_client)
    writer = S3ObjectCatalog(output_root)

    buckets = scanner.list_buckets_with_prefix(bucket_prefix)
    for bucket in buckets:
        logger.info(f"Scanning bucket: {bucket}")
        s3_objects = scanner.get_bucket_objects(bucket)
        writer.output_s3_objects_to_tsv(s3_objects, bucket, tsv_file_prefix)
    logger.info("Scan completed successfully.")


class BucketScanner:
    """
    Class to scan S3 buckets and iterate their objects.
    """

    def __init__(self, s3_client=None):
        """
        Initialize the BucketScanner with an optional S3 client.
        :param s3_client: Boto3 S3 client
        """
        self.s3_client = s3_client or boto3.client("s3")

    def list_buckets_with_prefix(self, prefix: str | None = None) -> list[str]:
        """
        List all S3 buckets with an optional prefix.
        :param prefix: Optional prefix to filter bucket names
        :return: List of bucket names
        """
        if not isinstance(prefix, str | type(None)):
            raise TypeError("Prefix must be a string or None")
        kwargs = {"Prefix": prefix} if prefix else {}
        response = self.s3_client.list_buckets(**kwargs)
        if not isinstance(response, dict):
            raise TypeError(f"Response must be a dictionary: {response=}")
        buckets = response.get("Buckets", [])
        return [b["Name"] for b in buckets]

    def get_bucket_objects(self, bucket_name: str) -> Generator[dict, None, None]:
        """
        Get all objects in an S3 bucket as a generator.
        :param bucket_name: Name of the S3 bucket
        :yield: All objects in the bucket
        """
        if not isinstance(bucket_name, str):
            raise TypeError("Bucket name must be a string")
        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterable = paginator.paginate(Bucket=bucket_name)
        for page in page_iterable:
            yield from page.get("Contents", [])

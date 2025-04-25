import csv
from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import Generator, Iterable

import boto3


logger = getLogger(__name__)
OBJ_KEY_MAP = {
    "LastModified": "last_modified",
    "Size": "size",
    "StorageClass": "storage_class",
    "ETag": "e_tag",
    "ChecksumAlgorithm": "checksum_algorithm",
    "ChecksumType": "checksum_type",
    "Key": "key",
}
TSV_FIELDS = [OBJ_KEY_MAP[k] for k in OBJ_KEY_MAP]


def run_s3_object_scan(
    bucket_prefix: str | None = None,
    parent_dir: str | Path | None = None,
    s3_client=None,
) -> None:
    """
    Run a scan of S3 buckets and output their objects to TSV files.
    :param bucket_prefix: Optional prefix to filter bucket names
    :param s3_client: Boto3 S3 client
    :param parent_dir: Parent directory for output files
    """
    if not isinstance(bucket_prefix, (str, type(None))):
        raise TypeError("Bucket prefix must be a string or None")
    if not isinstance(parent_dir, (str, Path, type(None))):
        raise TypeError("Parent directory must be a string or Path or None")

    scanner = BucketScanner(s3_client)
    writer = S3ObjectWriter(parent_dir)

    buckets = scanner.list_buckets_with_prefix(bucket_prefix)
    for bucket in buckets:
        logger.info(f"Scanning bucket: {bucket}")
        s3_objects = scanner.get_bucket_objects(bucket)
        writer.output_s3_objects_to_tsv(s3_objects, bucket)
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
        if not isinstance(prefix, (str, type(None))):
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
            for obj in page.get("Contents", []):
                yield obj


class S3ObjectWriter:
    """
    Class to handle writing S3 objects to TSV files.
    """

    def __init__(self, parent_dir: str | Path | None = None):
        """
        Initialize the S3ObjectWriter with an optional parent directory.
        :param parent_dir: Parent directory for output files
        """
        self.parent_dir = (
            Path(parent_dir).resolve() if parent_dir else Path("bucket-scans").resolve()
        )

    def output_s3_objects_to_tsv(
        self, s3_objects: Iterable[dict], bucket_name: str
    ) -> None:
        """
        Output S3 objects to a TSV file.
        Create the parent directory if it doesn't exist.
        :param s3_objects: Iterable of S3 objects
        :param bucket_name: Name of the S3 bucket
        """
        if not isinstance(s3_objects, Iterable):
            raise TypeError("S3 objects must be an iterable")
        if not isinstance(bucket_name, str):
            raise TypeError("Bucket name must be a string")
        tsv_file_path = self.new_tsv_file_path(bucket_name)
        self.ensure_parent_directory(tsv_file_path)
        with open(tsv_file_path, "w", newline="", encoding="utf-8") as tsv_file:
            writer = csv.DictWriter(tsv_file, fieldnames=TSV_FIELDS, delimiter="\t")
            writer.writeheader()
            for obj in s3_objects:
                writer.writerow({OBJ_KEY_MAP[k]: flatten(v) for k, v in obj.items()})

    def new_tsv_file_path(self, bucket_name: str) -> Path:
        """
        Generate a new TSV file path based on the bucket name and current timestamp.
        :param bucket_name: Name of the S3 bucket
        :return: Path to the new TSV file
        """
        if not isinstance(bucket_name, str):
            raise TypeError("Bucket name must be a string")
        if not bucket_name:
            raise ValueError("Bucket name is required")
        return (
            self.parent_dir
            / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{bucket_name}.tsv"
        )

    def ensure_parent_directory(self, tsv_file_path):
        """
        Ensure the parent directory of the TSV file exists.
        :param tsv_file_path: Path to the TSV file
        """
        if not isinstance(tsv_file_path, (str, Path)):
            raise TypeError("TSV file path must be a string or Path")
        if not tsv_file_path:
            raise ValueError("TSV file path is required")
        parent_dir = Path(tsv_file_path).parent.resolve()
        if not parent_dir.exists():
            parent_dir.mkdir(parents=True, exist_ok=True)
        if not parent_dir.is_dir():
            raise ValueError(f"{parent_dir} must be a directory")


def flatten(value):
    """
    Flatten a value to a string.
    :param value: Value to flatten
    :return: Flattened value as a string
    """
    match value:
        case list():
            return ":".join(value)
        case datetime():
            return value.isoformat()
        case str():
            if value[0] == value[-1] == '"':
                return value[1:-1]
            else:
                return value
        case _:
            return str(value)

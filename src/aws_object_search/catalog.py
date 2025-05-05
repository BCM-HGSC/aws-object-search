"""
Local catalog of the contents of cloud objects (S3/glacier).
"""

import csv
from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import Iterable


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


class S3ObjectCatalog:
    """
    Stores a catalog of S3 objects in TSV files.
    """

    def __init__(self, parent_dir: str | Path | None = None):
        """
        Initialize the S3ObjectCatalog with an optional parent directory.
        :param parent_dir: Parent directory for output files
        """
        self.parent_dir = (
            Path(parent_dir).resolve() if parent_dir else Path("bucket-scans").resolve()
        )

    def output_s3_objects_to_tsv(
        self, s3_objects: Iterable[dict], bucket_name: str, prefix: str | None = None
    ) -> None:
        """
        Output S3 objects to a TSV file.
        Create the parent directory if it doesn't exist.
        :param s3_objects: Iterable of S3 objects
        :param bucket_name: Name of the S3 bucket
        :param prefix: optional value to use instead of timestamp in output file names
        """
        assert isinstance(s3_objects, Iterable)
        assert isinstance(bucket_name, str)
        tsv_file_path = self.new_tsv_file_path(bucket_name, prefix)
        self.ensure_parent_directory(tsv_file_path)
        with open(tsv_file_path, "w", newline="", encoding="utf-8") as tsv_file:
            writer = csv.DictWriter(tsv_file, fieldnames=TSV_FIELDS, delimiter="\t")
            writer.writeheader()
            for obj in s3_objects:
                writer.writerow({OBJ_KEY_MAP[k]: flatten(v) for k, v in obj.items()})

    def new_tsv_file_path(self, bucket_name: str, prefix: str | None = None) -> Path:
        """
        Generate a new TSV file path based on the bucket name and current timestamp.
        :param bucket_name: Name of the S3 bucket
        :param prefix: optional value to use instead of timestamp in output file names
        :return: Path to the new TSV file
        """
        assert isinstance(bucket_name, str)
        assert bucket_name
        if not prefix:
            prefix = datetime.now().strftime("%Y%m%d-%H%M%S")
        return self.parent_dir / f"{prefix}-{bucket_name}.tsv"

    def ensure_parent_directory(self, tsv_file_path):
        """
        Ensure the parent directory of the TSV file exists.
        :param tsv_file_path: Path to the TSV file
        """
        assert isinstance(tsv_file_path, (str, Path))
        assert tsv_file_path
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

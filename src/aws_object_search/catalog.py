"""
Local catalog of the contents of cloud objects (S3/glacier).
"""

import csv
import gzip
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime
from logging import getLogger
from operator import attrgetter
from pathlib import Path
from typing import Any

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


@dataclass(frozen=True, order=True)
class ObjectMetadata:
    "Metadata for an object where all values are str. See flatten()."

    key: str
    last_modified: str
    size: str
    storage_class: str
    e_tag: str
    checksum_algorithm: str
    checksum_type: str

    def flattened_dict(self) -> dict[str, str]:
        "Fatten to str-based dict"
        return asdict(self)


@dataclass(frozen=True, order=True)
class BucketScan:
    """
    Wraps a TSV file that contains the results from scanning a particular bucket at
    a particular time. The TSV file may be gzipped. If so, file_path must have the ".gz"
    suffix.
    """

    file_path: Path

    @property
    def bucket_name(self) -> str:
        "Return name of the bucket, parsed from file_path."
        # Can't use stem, because it only removes the last suffix.
        return self.file_path.stem.split(".", 1)[0].split("-", 2)[2]

    @property
    def scan_start(self) -> datetime:
        "Return datetime for scan start, parsed from file_path."
        assert self.file_path.name[15] == "-"
        return datetime.strptime(self.file_path.name[:15], "%Y%m%d-%H%M%S")

    def contents(self) -> Iterable[ObjectMetadata]:
        "Iterate the underlying file's contents."
        o = gzip.open if self.file_path.suffix == ".gz" else open
        with o(self.file_path, "rt", newline="", encoding="utf-8") as tsv_file:
            reader = csv.DictReader(tsv_file, delimiter="\t")
            for row in reader:
                yield ObjectMetadata(**row)

    def flattened_dict(self) -> dict[str, str]:
        "Fatten to str-based dict"
        return {
            "scan_start": self.scan_start.isoformat(),
            "bucket_name": self.bucket_name,
        }


class S3ObjectCatalog:
    """
    Stores a catalog of S3 objects in TSV files in a common directory.
    """

    def __init__(self, catalog_root: str | Path):
        """
        Initialize the S3ObjectCatalog with an optional parent directory.
        :param catalog_root: Parent directory for output files
        """
        assert isinstance(catalog_root, str | Path), catalog_root
        if not catalog_root:
            raise ValueError("catalog_root connot be empty string")
        self.catalog_root = Path(catalog_root).resolve()

    def iter_dicts(self) -> Iterable[dict[str, str]]:
        "Iterate all current contents flattened to dict and str"
        for bucket_scan, object_metadata in self.current_contents():
            yield bucket_scan.flattened_dict() | object_metadata.flattened_dict()

    def current_contents(self) -> Iterable[tuple[BucketScan, ObjectMetadata]]:
        "Yield everything current"
        for bucket_scan in self.current_bucket_scans():
            for object_metadata in bucket_scan.contents():
                yield (bucket_scan, object_metadata)

    def current_bucket_scans(self) -> list[BucketScan]:
        """
        Return list sorted by scan_start of only the most recent scan for each bucket.
        """
        most_recent_scans: dict[str, BucketScan] = {}
        for b in self.all_bucket_scans():
            if (
                b.bucket_name not in most_recent_scans
                or b.scan_start > most_recent_scans[b.bucket_name].scan_start
            ):
                most_recent_scans[b.bucket_name] = b
        return sorted(
            most_recent_scans.values(), key=attrgetter("scan_start", "bucket_name")
        )

    def all_bucket_scans(self) -> Iterable[BucketScan]:
        """Yield all scans in catalog, .tsv files before .tsv.gz files."""
        patterns = ["????????-??????-*.tsv", "????????-??????-*.tsv.gz"]
        for pattern in patterns:
            for file_path in self.catalog_root.glob(pattern):
                yield BucketScan(file_path)

    def archive_old_scans(self) -> None:
        """
        Move old (non-current) scan files to archive directory organized by date.
        Archive path format: {catalog_root}/archive/{year}/{month}/{day}/
        """
        current_scans = {b.file_path for b in self.current_bucket_scans()}
        old_scans = [
            b for b in self.all_bucket_scans() if b.file_path not in current_scans
        ]

        if not old_scans:
            logger.info("No old scans to archive")
            return

        for scan in old_scans:
            # Extract year, month, day from scan_start
            year = scan.scan_start.strftime("%Y")
            month = scan.scan_start.strftime("%m")
            day = scan.scan_start.strftime("%d")

            # Create archive directory path
            archive_dir = self.catalog_root / "archive" / year / month / day
            archive_dir.mkdir(parents=True, exist_ok=True)

            # Move file to archive
            destination = archive_dir / scan.file_path.name
            logger.info(f"Archiving {scan.file_path.name} to {archive_dir}")
            try:
                scan.file_path.rename(destination)
            except FileExistsError:
                logger.warning(f"Destination already exists, skipping: {destination}")
            except OSError as e:
                logger.error(f"Failed to archive {scan.file_path.name}: {e}")

    def output_s3_objects_to_tsv(
        self,
        s3_objects: Iterable[dict[str, Any]],
        bucket_name: str,
        prefix: str | None = None,
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
        tsv_file_path = self.new_tsv_gz_file_path(bucket_name, prefix)
        self.ensure_catalog_root(tsv_file_path)
        with gzip.open(tsv_file_path, "wt", newline="", encoding="utf-8") as tsv_file:
            writer = csv.DictWriter(tsv_file, fieldnames=TSV_FIELDS, delimiter="\t")
            writer.writeheader()
            for obj in s3_objects:
                writer.writerow(
                    # By using the get method with default,
                    # handle cases with AWS names or Python names
                    {OBJ_KEY_MAP.get(k, k): flatten(v) for k, v in obj.items()}
                )

    def new_tsv_gz_file_path(self, bucket_name: str, prefix: str | None = None) -> Path:
        """
        Generate a new TSV.GZ file path based on the bucket name and current timestamp.
        :param bucket_name: Name of the S3 bucket
        :param prefix: optional value to use instead of timestamp in output file names
        :return: Path to the new TSV file
        """
        assert isinstance(bucket_name, str)
        assert bucket_name
        if not prefix:
            prefix = datetime.now().strftime("%Y%m%d-%H%M%S")
        return self.catalog_root / f"{prefix}-{bucket_name}.tsv.gz"

    def ensure_catalog_root(self, tsv_file_path):
        """
        Ensure the parent directory of the TSV file exists.
        :param tsv_file_path: Path to the TSV file
        """
        assert isinstance(tsv_file_path, str | Path)
        assert tsv_file_path
        catalog_root = Path(tsv_file_path).parent.resolve()
        if not catalog_root.exists():
            catalog_root.mkdir(parents=True, exist_ok=True)
        if not catalog_root.is_dir():
            raise ValueError(f"{catalog_root} must be a directory")


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

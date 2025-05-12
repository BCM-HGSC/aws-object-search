import argparse
from pathlib import Path
from logging import getLogger
from sys import exit

import botocore.exceptions

from . import __version__
from .logging import config_logging
from .s3_wrapper import run_s3_object_scan


logger = getLogger(__name__)


def aos_scan(args: argparse.Namespace | None = None) -> None:
    "Run a scan of S3 buckets and output their objects to TSV files."
    if args is None:
        args = parse_scan_args()
    config_logging(args.log_level)
    logger.info(f"Bucket prefix: {args.bucket_prefix}")
    logger.info(f"Parent directory: {args.parent_dir}")
    logger.info("Scanning AWS Objects...")
    try:
        run_s3_object_scan(args.bucket_prefix, args.parent_dir)
    except botocore.exceptions.TokenRetrievalError as e:
        logger.error(f"Failed to retrieve S3 buckets: {e}")
        exit("Possibly not logged in")
    logger.info("Scan completed successfully.")


def parse_scan_args() -> argparse.Namespace:
    "Parse command line arguments."
    parser = argparse.ArgumentParser(
        description="Scan AWS S3 buckets and output their objects to TSV files."
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show the version of the program",
    )
    parser.add_argument(
        "--bucket-prefix",
        default=None,
        help="Optional prefix to filter bucket names",
    )
    parser.add_argument(
        "--parent-dir",
        type=Path,
        default=None,
        help="Parent directory for output files (default: current directory)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (default: INFO)",
    )
    return parser.parse_args()

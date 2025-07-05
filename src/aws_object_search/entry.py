import argparse
from logging import getLogger
from pathlib import Path
from sys import exit, stderr

import botocore.exceptions

from . import __version__
from .logging import config_logging
from .s3_wrapper import run_s3_object_scan
from .tantivy_wrapper import index_catalog, search_index, search_index_simple


logger = getLogger(__name__)


def aos_scan(args: argparse.Namespace | None = None) -> None:
    "Run a scan of S3 buckets and output their objects to TSV files."
    if args is None:
        args = parse_scan_args()
    config_logging(args.log_level)
    logger.info(f"Bucket prefix: {args.bucket_prefix}")
    logger.info(f"Parent directory: {args.parent_dir}")
    logger.info(f"Scanning: {not args.no_scan}")
    logger.info(f"Indexing: {not args.no_index}")
    if not args.no_scan:
        logger.info("Scanning AWS Objects...")
        try:
            run_s3_object_scan(
                args.parent_dir,
                args.bucket_prefix,
            )
        except botocore.exceptions.TokenRetrievalError as e:
            logger.error(f"Failed to retrieve S3 buckets: {e}")
            exit("Possibly not logged in")
        else:
            logger.info("Scan completed successfully.")
    if not args.no_index:
        logger.info("Indexing S3 objects...")
        index_catalog(args.parent_dir, args.parent_dir / "index")


def parse_scan_args() -> argparse.Namespace:
    "Parse command line arguments."
    parser = argparse.ArgumentParser(
        description="Scan AWS S3 buckets, list their key in TSV files, "
        "and index the results."
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
        default=Path("./s3_objects"),
        help="Parent directory for output files (default: ./s3_objects)",
    )
    parser.add_argument(
        "--no-scan",
        action="store_true",
        help="Suppress scanning",
    )
    parser.add_argument(
        "--no-index",
        action="store_true",
        help="Suppress indexing",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (default: INFO)",
    )
    return parser.parse_args()


def aos_search(args: argparse.Namespace | None = None) -> None:
    "Entry point for searching the index."
    if args is None:
        args = parse_search_args()
    config_logging(args.log_level)
    logger.info(f"Parent directory: {args.parent_dir}")
    logger.info(f"Query string: '{args.query}'")
    try:
        search_index(args.parent_dir / "index", args.query)
    except BrokenPipeError:
        pass  # normal; for example, piped to "head" command
    stderr.close()
    exit(0)


def parse_search_args() -> argparse.Namespace:
    "Parse command line arguments."
    parser = argparse.ArgumentParser(
        description="Search index of keys in AWS S3 buckets."
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show the version of the program",
    )
    parser.add_argument(
        "parent_dir",
        type=Path,
        help="Directory containing the scan output files",
    )
    parser.add_argument(
        "query",
        help="Query string to search for",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="WARNING",
        help="Logging level (default: WARNING)",
    )
    return parser.parse_args()


def search_aws(args: argparse.Namespace | None = None) -> None:
    "Entry point for searching the index with simple output."
    if args is None:
        args = parse_search_aws_args()
    config_logging(args.log_level)
    logger.info(f"Parent directory: {args.parent_dir}")
    logger.info(f"Query string: '{args.query}'")
    try:
        search_index_simple(
            args.parent_dir / "index",
            args.query,
            latest=args.latest,
            no_file_sizes=args.no_file_sizes,
        )
    except BrokenPipeError:
        pass  # normal; for example, piped to "head" command
    stderr.close()
    exit(0)


def parse_search_aws_args() -> argparse.Namespace:
    "Parse command line arguments for search-aws."
    parser = argparse.ArgumentParser(
        description="Search index of keys in AWS S3 buckets with simple output."
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show the version of the program",
    )
    parser.add_argument(
        "query",
        help="Query string to search for",
    )
    parser.add_argument(
        "-l",
        "--latest",
        action="store_true",
        help="Select the most recent matches for each input line",
    )
    parser.add_argument(
        "-m",
        "--max-results-per-query",
        type=int,
        default=10000000,
        help="Maximum results per query (default: 10000000)",
    )
    parser.add_argument(
        "-s",
        "--no-file-sizes",
        action="store_true",
        help="Suppress including size of files in the output",
    )
    parser.add_argument(
        "--parent-dir",
        type=Path,
        default=Path("./s3_objects"),
        help="Directory containing the scan output files (default: ./s3_objects)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="WARNING",
        help="Logging level (default: WARNING)",
    )
    return parser.parse_args()

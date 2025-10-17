import argparse
import fcntl
from logging import getLogger
from pathlib import Path
from sys import exit, prefix, stderr

import botocore.exceptions

from . import __version__
from .logging import config_logging
from .s3_wrapper import run_s3_object_scan
from .tantivy_wrapper import index_catalog, run_query

logger = getLogger(__name__)

# Default directory for catalog and index files
DEFAULT_OUTPUT_ROOT = Path(prefix).resolve().parent / "s3_objects"

# File endings for filtering search results
RAW_READS_ENDINGS = [
    "_sequence.txt.bz2",
    "_sequence.txt.gz",
    "_sequence.txt",
    ".fastq.gz",
    "_001.fastq.gz",
]

CONFIG_ENDINGS = [
    "BWAConfigParams.txt",
    ".config.csv",
    "config.txt",
    "event.json",
    "FCDefn.json",
    "SEDefn.json",
    "MEDefn.json",
    "MergeDefn.json",
]

BAM_ENDINGS = [
    "_realigned.bam",
    ".realigned.recal.bam",
    ".recal.realigned.bam",
    ".hgv.bam",
]

CRAM_ENDINGS = [
    ".hgv.cram",
]

VCF_ENDINGS = [
    ".SNPs_Annotated.vcf",
    "_snp.vcf.gz",
    ".INDELs_Annotated.vcf",
    "_indel.vcf.gz",
]

BAM_INDEX_ENDINGS = ["bam.bai"]
CRAM_INDEX_ENDINGS = ["cram.crai"]
VCF_INDEX_ENDINGS = ["vcf.gz.tbi"]


def aos_scan(args: argparse.Namespace | None = None) -> None:
    "Run a scan of S3 buckets and output their objects to TSV files."
    if args is None:
        args = parse_scan_args()
    config_logging(args.log_level)

    # Acquire lock if flock option is specified
    lock_file = None
    if args.flock is not None:
        try:
            # Open lock file for writing (create if doesn't exist)
            lock_file = open(args.flock, "w")
            # Attempt to acquire exclusive lock (non-blocking)
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            logger.info(f"Acquired lock on {args.flock}")
        except BlockingIOError:
            logger.critical(
                f"Another aos-scan process is already running "
                f"(lock file: {args.flock}). Exiting."
            )
            if lock_file is not None:
                lock_file.close()
            exit(2)
        except Exception as e:
            logger.critical(f"Failed to acquire lock on {args.flock}: {e}")
            if lock_file is not None:
                lock_file.close()
            exit(2)

    try:
        logger.info(f"Bucket prefix: {args.bucket_prefix}")
        logger.info(f"Output root: {args.output_root}")
        logger.info(f"Scanning: {not args.no_scan}")
        logger.info(f"Indexing: {not args.no_index}")
        if not args.no_scan:
            logger.info("Scanning AWS Objects...")
            try:
                run_s3_object_scan(
                    args.output_root,
                    args.bucket_prefix,
                )
            except botocore.exceptions.TokenRetrievalError as e:
                logger.error(f"Failed to retrieve S3 buckets: {e}")
                exit("Possibly not logged in")
            else:
                logger.info("Scan completed successfully.")
        if not args.no_index:
            logger.info("Indexing S3 objects...")
            index_catalog(args.output_root, args.output_root / "index")
    finally:
        # Release lock and close lock file
        if lock_file is not None:
            lock_file.close()
            logger.info(f"Released lock on {args.flock}")


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
        "-o",
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Output root directory for generated files "
        f"(default: {DEFAULT_OUTPUT_ROOT})",
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
    parser.add_argument(
        "-f",
        "--flock",
        type=Path,
        default=None,
        help="Path to lock file for preventing concurrent scans "
        "(optional, no locking if not specified)",
    )
    return parser.parse_args()


def search_aws(args: argparse.Namespace | None = None) -> None:
    "Entry point for searching the index with simple output."
    if args is None:
        args = parse_search_aws_args()
    config_logging(args.log_level)
    logger.info(f"Output root: {args.output_root}")
    logger.info(f"Query string: '{args.query}'")

    warn_about_flag_conflicts(args)

    # Build file endings filter based on command-line args
    file_endings = build_file_endings_filter(args)

    try:
        results = run_query(
            args.output_root / "index", args.query, args.max_results_per_query
        )

        for _score, doc in results:
            s3_uri = f"s3://{doc.bucket_name}/{doc.key}"

            # Apply file ending filter
            if not filter_by_file_endings(s3_uri, file_endings):
                continue

            format_and_write_result(doc, args.uri_only)

    except BrokenPipeError:
        pass  # normal; for example, piped to "head" command
    stderr.close()
    exit(0)


def parse_search_aws_args() -> argparse.Namespace:
    "Parse command line arguments for search-aws."
    parser = argparse.ArgumentParser(
        description="Search index of keys in AWS S3 buckets with simple output.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  search-aws "search_string"
  search-aws "*.fastq" -u
  search-aws "SIC1234.*cram" -m 1000

Output format:
  Unless --uri-only is specified, search results are returned as tab-separated values:

  Standard columns:
    s3_uri        size        last_modified        storage_class

  With --uri-only, only S3 URIs are printed:
    s3://bucket-name/path/to/file

  Matching files are printed to standard output.
""",
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
        "-m",
        "--max-results-per-query",
        type=int,
        default=10_000_000,
        help="Maximum results per query (default: 10,000,000)",
    )
    parser.add_argument(
        "-o",
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Output root directory containing the scan files (default: "
        f"{DEFAULT_OUTPUT_ROOT})",
    )
    parser.add_argument(
        "-u",
        "--uri-only",
        action="store_true",
        help="Suppress all output except for the S3 URIs",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="WARNING",
        help="Logging level (default: WARNING)",
    )
    add_file_type_filter_arguments(parser)
    return parser.parse_args()


def search_py(args: argparse.Namespace | None = None) -> None:
    "Entry point for search.py command - processes input file with search terms."
    if args is None:
        args = parse_search_py_args()
    config_logging(args.log_level)
    logger.info(f"Output root: {args.output_root}")
    logger.info(f"Input file: '{args.file}'")

    warn_about_flag_conflicts(args)

    # Build file endings filter based on command-line args
    file_endings = build_file_endings_filter(args)

    # Read search terms from input file
    try:
        with open(args.file) as f:
            search_terms = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logger.error(f"Input file not found: {args.file}")
        exit(1)
    except OSError as e:
        logger.error(f"Error reading input file: {e}")
        exit(1)

    # Prepare output files
    input_path = Path(args.file)
    output_files = {
        "out": input_path.with_suffix(input_path.suffix + ".out.tsv"),
        "info": input_path.with_suffix(input_path.suffix + ".out.info"),
        "not_found": input_path.with_suffix(input_path.suffix + ".not_found.txt"),
        "not_found_list": input_path.with_suffix(input_path.suffix + ".not_found.list"),
    }

    not_found_terms = []
    total_matches = 0

    try:
        with (
            open(output_files["out"], "w") as out_file,
            open(output_files["info"], "w") as info_file,
            open(output_files["not_found"], "w") as not_found_file,
            open(output_files["not_found_list"], "w") as not_found_list_file,
        ):
            # Write headers
            info_file.write("# Search results summary\n")
            info_file.write(f"# Input file: {args.file}\n")
            info_file.write(f"# Total search terms: {len(search_terms)}\n")

            for term in search_terms:
                logger.info(f"Searching for: '{term}'")
                try:
                    results = list(
                        run_query(
                            args.output_root / "index", term, args.max_results_per_query
                        )
                    )

                    if results:
                        # Apply file ending filter to results
                        filtered_results = [
                            (_score, doc)
                            for _score, doc in results
                            if filter_by_file_endings(
                                f"s3://{doc.bucket_name}/{doc.key}", file_endings
                            )
                        ]

                        match_count = len(filtered_results)
                        total_matches += match_count

                        if filtered_results:
                            info_file.write(f"{term}\t{match_count} matches\n")

                            for _score, doc in filtered_results:
                                format_and_write_result(doc, args.uri_only, out_file)
                        else:
                            # No results after filtering
                            record_not_found_term(
                                term,
                                not_found_terms,
                                not_found_file,
                                not_found_list_file,
                                info_file,
                            )
                    else:
                        record_not_found_term(
                            term,
                            not_found_terms,
                            not_found_file,
                            not_found_list_file,
                            info_file,
                        )

                except Exception as e:
                    logger.error(f"Error searching for '{term}': {e}")
                    not_found_terms.append(term)
                    not_found_file.write(f"{term}\tError: {e}\n")
                    not_found_list_file.write(f"{term}\n")

            # Write summary to info file
            info_file.write(f"# Total matches found: {total_matches}\n")
            info_file.write(f"# Terms with no matches: {len(not_found_terms)}\n")

    except OSError as e:
        logger.error(f"Error writing output files: {e}")
        exit(1)

    logger.info(f"Search completed. Results written to {output_files['out']}")
    logger.info(f"Summary written to {output_files['info']}")
    if not_found_terms:
        logger.info(f"Terms with no matches written to {output_files['not_found']}")
    exit(0)


def parse_search_py_args() -> argparse.Namespace:
    "Parse command line arguments for search.py."
    parser = argparse.ArgumentParser(
        description="Search index of keys in AWS S3 buckets using input file "
        "with search terms.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  search.py inputlist.txt
  search.py -f inputlist.txt
  search.py inputlist.txt --uri-only

Output files:
  FILE.out.tsv        - Tab-separated list of matching S3 objects
  FILE.out.info       - Extra information (match count and date archived)
  FILE.not_found.txt  - Input lines that did not match (with errors)
  FILE.not_found.list - Clean list of input lines that did not match

Output format:
  Unless --uri-only is specified, FILE.out.tsv contains tab-separated values:

  Standard columns:
    s3_uri        size        last_modified        storage_class

  With --uri-only, FILE.out.tsv contains only S3 URIs:
    s3://bucket-name/path/to/file

  Note: FILE is the path of input file as provided by the user.
""",
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show the version of the program",
    )
    parser.add_argument(
        "file",
        nargs="?",
        help="Input file containing search terms, one per line",
    )
    parser.add_argument(
        "-f",
        "--file",
        dest="file_alt",
        help="Input file (for backwards compatibility - "
        "use positional argument instead)",
    )
    parser.add_argument(
        "-m",
        "--max-results-per-query",
        type=int,
        default=10_000_000,
        help="Maximum results per query (default: 10,000,000)",
    )
    parser.add_argument(
        "-o",
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Output root directory containing the scan files "
        f"(default: {DEFAULT_OUTPUT_ROOT})",
    )
    parser.add_argument(
        "-u",
        "--uri-only",
        action="store_true",
        help="Suppress all output except for the S3 URIs",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="WARNING",
        help="Logging level (default: WARNING)",
    )
    add_file_type_filter_arguments(parser)
    args = parser.parse_args()

    # Handle backwards compatibility: use -f/--file if provided,
    # otherwise use positional
    if args.file_alt:
        args.file = args.file_alt

    # Ensure we have a file specified
    if not args.file:
        parser.error(
            "Input file is required (either as positional argument or with -f/--file)"
        )

    return args


# Helper functions for file type filtering


def build_file_endings_filter(args: argparse.Namespace) -> list[str] | None:
    """
    Build list of allowed file endings based on command-line args.
    Returns None if no filtering should be applied (--all flag without file types).
    Returns list of endings for filtering by default or with specific flags.
    Explicit file type flags take precedence over --all.
    """
    # Check if any explicit file type flags are specified
    any_file_types_specified = any(
        [
            args.raw_reads,
            args.mapped_reads,
            args.bam,
            args.cram,
            args.vcf,
            args.configs,
        ]
    )

    # --all only applies when no explicit file types are specified
    if args.all and not any_file_types_specified:
        return None

    selected_endings = []

    # If no type flags specified, use default (equivalent to -gprv)
    no_flags_specified = not any_file_types_specified

    # Handle --raw-reads or default
    if args.raw_reads or no_flags_specified:
        selected_endings.extend(RAW_READS_ENDINGS)

    # Handle --configs or default
    if args.configs or no_flags_specified:
        selected_endings.extend(CONFIG_ENDINGS)

    # Handle --mapped-reads (includes both BAM and CRAM)
    if args.mapped_reads or no_flags_specified:
        selected_endings.extend(BAM_ENDINGS)
        selected_endings.extend(CRAM_ENDINGS)
        if not args.no_index:
            selected_endings.extend(BAM_INDEX_ENDINGS)
            selected_endings.extend(CRAM_INDEX_ENDINGS)
    else:
        # Handle individual --bam flag
        if args.bam:
            selected_endings.extend(BAM_ENDINGS)
            if not args.no_index:
                selected_endings.extend(BAM_INDEX_ENDINGS)

        # Handle individual --cram flag
        if args.cram:
            selected_endings.extend(CRAM_ENDINGS)
            if not args.no_index:
                selected_endings.extend(CRAM_INDEX_ENDINGS)

    # Handle --vcf or default
    if args.vcf or no_flags_specified:
        selected_endings.extend(VCF_ENDINGS)
        if not args.no_index:
            selected_endings.extend(VCF_INDEX_ENDINGS)

    return selected_endings if selected_endings else None


def filter_by_file_endings(uri: str, endings: list[str] | None) -> bool:
    """
    Check if URI ends with one of the allowed endings.
    If endings is None, all URIs pass through (no filtering).
    Returns True if URI should be included in results.
    """
    if endings is None:
        return True

    for ending in endings:
        if uri.endswith(ending):
            return True

    return False


def add_file_type_filter_arguments(parser: argparse.ArgumentParser) -> None:
    """Add file type filtering arguments to an argument parser."""
    parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="Show all results without filtering",
    )
    parser.add_argument(
        "-r",
        "--raw-reads",
        action="store_true",
        help="Include raw read files (FASTQ)",
    )
    parser.add_argument(
        "-p",
        "--mapped-reads",
        action="store_true",
        help="Include mapped read files (BAM and CRAM with indexes)",
    )
    parser.add_argument(
        "-b",
        "--bam",
        action="store_true",
        help="Include BAM files and their indexes",
    )
    parser.add_argument(
        "-c",
        "--cram",
        action="store_true",
        help="Include CRAM files and their indexes",
    )
    parser.add_argument(
        "-v",
        "--vcf",
        action="store_true",
        help="Include VCF files and their indexes",
    )
    parser.add_argument(
        "-g",
        "--configs",
        action="store_true",
        help="Include configuration files",
    )
    parser.add_argument(
        "-n",
        "--no-index",
        action="store_true",
        help="Exclude index files (.bai, .crai, .tbi)",
    )


def format_and_write_result(doc, uri_only: bool, output_file=None) -> None:
    """
    Format and write a search result document.

    Args:
        doc: Document with bucket_name, key, size, last_modified, storage_class
        uri_only: If True, output only the S3 URI
        output_file: File handle to write to, or None to print to stdout
    """
    bucket_name = doc.bucket_name
    key = doc.key
    s3_uri = f"s3://{bucket_name}/{key}"

    if uri_only:
        line = s3_uri
    else:
        size = doc.size
        last_modified = doc.last_modified
        storage_class = doc.storage_class
        line = f"{s3_uri}\t{size}\t{last_modified}\t{storage_class}"

    if output_file is None:
        print(line)
    else:
        output_file.write(f"{line}\n")


def record_not_found_term(
    term: str,
    not_found_terms: list,
    not_found_file,
    not_found_list_file,
    info_file,
) -> None:
    """Record a search term that had no matches in the output files."""
    not_found_terms.append(term)
    not_found_file.write(f"{term}\t0 matches\n")
    not_found_list_file.write(f"{term}\n")
    info_file.write(f"{term}\t0 matches\n")


def warn_about_flag_conflicts(args: argparse.Namespace) -> None:
    """Warn about problematic flag combinations."""
    # Warn about -m without --all (filtering happens after limit)
    if args.max_results_per_query != 10_000_000 and not args.all:
        print(
            "Warning: --max-results-per-query limit is applied before "
            "file type filtering. Use --all with -m for predictable results.",
            file=stderr,
        )

    # Warn about --all with conflicting file type flags
    if args.all and any(
        [
            args.raw_reads,
            args.mapped_reads,
            args.bam,
            args.cram,
            args.vcf,
            args.configs,
        ]
    ):
        print(
            "Warning: File type flags specified; ignoring --all flag.",
            file=stderr,
        )

import argparse
from logging import getLogger
from pathlib import Path
from sys import exit, prefix, stderr

import botocore.exceptions

from . import __version__
from .logging import config_logging
from .s3_wrapper import run_s3_object_scan
from .tantivy_wrapper import index_catalog, search_index_simple, run_query


logger = getLogger(__name__)

# Default directory for catalog and index files
DEFAULT_OUTPUT_ROOT = Path(prefix).resolve().parent / "s3_objects"


def aos_scan(args: argparse.Namespace | None = None) -> None:
    "Run a scan of S3 buckets and output their objects to TSV files."
    if args is None:
        args = parse_scan_args()
    config_logging(args.log_level)
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
        help=f"Output root directory for generated files (default: {DEFAULT_OUTPUT_ROOT})",
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



def search_aws(args: argparse.Namespace | None = None) -> None:
    "Entry point for searching the index with simple output."
    if args is None:
        args = parse_search_aws_args()
    config_logging(args.log_level)
    logger.info(f"Output root: {args.output_root}")
    logger.info(f"Query string: '{args.query}'")
    try:
        search_index_simple(
            args.output_root / "index",
            args.query,
            uri_only=args.uri_only,
            max_results=args.max_results_per_query,
        )
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
"""
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
        help=f"Output root directory containing the scan files (default: {DEFAULT_OUTPUT_ROOT})",
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
    return parser.parse_args()


def search_py(args: argparse.Namespace | None = None) -> None:
    "Entry point for search.py command - processes input file with search terms."
    if args is None:
        args = parse_search_py_args()
    config_logging(args.log_level)
    logger.info(f"Output root: {args.output_root}")
    logger.info(f"Input file: '{args.file}'")
    
    # Read search terms from input file
    try:
        with open(args.file, 'r') as f:
            search_terms = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logger.error(f"Input file not found: {args.file}")
        exit(1)
    except IOError as e:
        logger.error(f"Error reading input file: {e}")
        exit(1)
    
    # Prepare output files
    input_path = Path(args.file)
    output_files = {
        'out': input_path.with_suffix(input_path.suffix + '.out.tsv'),
        'info': input_path.with_suffix(input_path.suffix + '.out.info'),
        'not_found': input_path.with_suffix(input_path.suffix + '.not_found.txt'),
        'not_found_list': input_path.with_suffix(input_path.suffix + '.not_found.list'),
    }
    
    not_found_terms = []
    total_matches = 0
    
    try:
        with open(output_files['out'], 'w') as out_file, \
             open(output_files['info'], 'w') as info_file, \
             open(output_files['not_found'], 'w') as not_found_file, \
             open(output_files['not_found_list'], 'w') as not_found_list_file:
            
            # Write headers
            info_file.write("# Search results summary\n")
            info_file.write(f"# Input file: {args.file}\n")
            info_file.write(f"# Total search terms: {len(search_terms)}\n")
            
            for term in search_terms:
                logger.info(f"Searching for: '{term}'")
                try:
                    results = list(run_query(
                        args.output_root / "index",
                        term,
                        args.max_results_per_query
                    ))
                    
                    if results:
                        match_count = len(results)
                        total_matches += match_count
                        info_file.write(f"{term}\t{match_count} matches\n")
                        
                        for _score, doc in results:
                            bucket_name = doc.bucket_name
                            key = doc.key
                            s3_uri = f"s3://{bucket_name}/{key}"
                            
                            if args.uri_only:
                                out_file.write(f"{s3_uri}\n")
                            else:
                                size = doc.size
                                last_modified = doc.last_modified
                                storage_class = doc.storage_class
                                out_file.write(f"{s3_uri}\t{size}\t{last_modified}\t{storage_class}\n")
                    else:
                        not_found_terms.append(term)
                        not_found_file.write(f"{term}\t0 matches\n")
                        not_found_list_file.write(f"{term}\n")
                        info_file.write(f"{term}\t0 matches\n")
                        
                except Exception as e:
                    logger.error(f"Error searching for '{term}': {e}")
                    not_found_terms.append(term)
                    not_found_file.write(f"{term}\tError: {e}\n")
                    not_found_list_file.write(f"{term}\n")
            
            # Write summary to info file
            info_file.write(f"# Total matches found: {total_matches}\n")
            info_file.write(f"# Terms with no matches: {len(not_found_terms)}\n")
            
    except IOError as e:
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
        description="Search index of keys in AWS S3 buckets using input file with search terms.",
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
"""
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
        help="Input file (for backwards compatibility - use positional argument instead)",
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
        help=f"Output root directory containing the scan files (default: {DEFAULT_OUTPUT_ROOT})",
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
    args = parser.parse_args()
    
    # Handle backwards compatibility: use -f/--file if provided, otherwise use positional
    if args.file_alt:
        args.file = args.file_alt
    
    # Ensure we have a file specified
    if not args.file:
        parser.error("Input file is required (either as positional argument or with -f/--file)")
    
    return args

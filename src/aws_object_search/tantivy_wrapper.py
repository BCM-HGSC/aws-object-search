"""Business logic around tantivy."""

from dataclasses import dataclass
from logging import getLogger
from pathlib import Path
from shutil import rmtree
from typing import Iterable

import tantivy

from .catalog import S3ObjectCatalog


logger = getLogger(__name__)


@dataclass
class S3ObjectResult:
    """Data class representing an S3 object search result."""
    last_scan_timestamp: str = "MISSING"
    bucket_name: str = "MISSING"
    last_modified: str = "MISSING"
    size: str = "MISSING"
    storage_class: str = "MISSING"
    e_tag: str = "MISSING"
    checksum_algorithm: str = "MISSING"
    checksum_type: str = "MISSING"
    key: str = "MISSING"


def search_index(index_path: Path | str, query: str) -> None:
    "Search for query."
    for score, doc in run_query(index_path, query):
        bucket_name = doc.bucket_name
        key = doc.key
        print(f"{score:06.2f}", bucket_name, key, sep="\t")


def search_index_simple(
    index_path: Path | str,
    query: str,
    latest: bool = False,
) -> None:
    "Search for query with simple output format for search-aws."
    results = list(run_query(index_path, query))

    if latest:
        # Group by key and select most recent (highest score)
        key_groups = {}
        for score, doc in results:
            key = doc.key
            if key not in key_groups or score > key_groups[key]:
                key_groups[key] = (score, doc)
        results = list(key_groups.values())

    for _score, doc in results:
        bucket_name = doc.bucket_name
        key = doc.key
        s3_uri = f"s3://{bucket_name}/{key}"
        size = doc.size
        last_modified = doc.last_modified
        storage_class = doc.storage_class

        print(f"{s3_uri}\t{size}\t{last_modified}\t{storage_class}")


def run_query(
    index_path: Path | str, query_str: str
) -> Iterable[tuple[float, S3ObjectResult]]:
    "Search for query and generate (score, S3ObjectResult) pairs."
    schema = build_schema()
    index = tantivy.Index(schema, str(index_path))
    query_obj = index.parse_query(query_str, ["key"])
    searcher = index.searcher()
    # TODO: page results beyond 1000
    results = searcher.search(query_obj, 1000)

    for score, address in results.hits:
        doc = searcher.doc(address)
        doc_dict = doc.to_dict()

        # Create S3ObjectResult with default values
        result = S3ObjectResult()
        
        # Update fields from document
        for field in result.__dataclass_fields__:
            if field in doc_dict:
                value_list = doc_dict[field]
                if len(value_list) != 1:
                    logger.warning(f"abnormal value list for {field} in {doc_dict}")
                setattr(result, field, ";".join(doc_dict[field]))

        yield score, result


def index_catalog(catalog_root: Path, index_path: Path) -> None:
    "Populate a new index, replacing any existing index."
    catalog = S3ObjectCatalog(catalog_root)
    regenerate_index(index_path, catalog.iter_dicts())


def regenerate_index(index_path: Path, documents: Iterable[dict[str, str]]) -> None:
    "Populate a new index, replacing any existing index."
    # TODO: avoid external race condition.
    schema = build_schema()
    if index_path.is_dir():
        rmtree(index_path)
    index = create_index(schema, index_path)
    writer = index.writer()
    for d in documents:
        writer.add_document(tantivy.Document(**d))
    writer.commit()
    writer.wait_merging_threads()


def build_schema() -> tantivy.Schema:
    "Return schema matching scan output."
    schema_builder = tantivy.SchemaBuilder()
    schema_builder.add_text_field("last_scan_timestamp", stored=True)
    schema_builder.add_text_field("bucket_name", stored=True)
    schema_builder.add_text_field("last_modified", stored=True)
    schema_builder.add_text_field("size", stored=True)
    schema_builder.add_text_field("storage_class", stored=True)
    schema_builder.add_text_field("e_tag", stored=True)
    schema_builder.add_text_field("checksum_algorithm", stored=True)
    schema_builder.add_text_field("checksum_type", stored=True)
    schema_builder.add_text_field("key", stored=True)
    schema = schema_builder.build()
    return schema


def create_index(schema: tantivy.Schema, index_path: Path) -> tantivy.Index:
    "Create a Tantivy index at the specified path."
    index_path.mkdir(exist_ok=True)
    index = tantivy.Index(schema, path=str(index_path))
    return index

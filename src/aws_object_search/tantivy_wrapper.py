"""Business logic around tantivy."""

from logging import getLogger
from pathlib import Path
from shutil import rmtree
from typing import Iterable

import tantivy

from .catalog import S3ObjectCatalog


logger = getLogger(__name__)


def search_index(index_path: Path | str, query: str) -> None:
    "Search for query."
    for score, doc in run_query(index_path, query):
        print(f"{score:06.2f}", doc["bucket_name"][0], doc["key"][0], sep="\t")


def run_query(
    index_path: Path | str, query_str: str
) -> Iterable[tuple[float, dict[str, str]]]:
    "Search for query and generate (score, dict) pairs."
    schema = build_schema()
    index = tantivy.Index(schema, str(index_path))
    query_obj = index.parse_query(query_str, ["key"])
    searcher = index.searcher()
    # TODO: page results beyond 1000
    results = searcher.search(query_obj, 1000)
    for score, address in results.hits:
        doc = searcher.doc(address)
        yield score, doc.to_dict()


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

import pytest
import tantivy

from aws_object_search.tantivy_wrapper import (
    S3ObjectResult,
    build_schema,
    create_index,
    index_catalog,
    regenerate_index,
    run_query,
    search_index_simple,
)


@pytest.fixture
def sample_documents():
    """Fixture providing sample document data for testing."""
    return [
        {
            "last_scan_timestamp": "2025-05-04T16:48:32",
            "bucket_name": "test-bucket-1",
            "last_modified": "2025-03-31T01:37:05+00:00",
            "size": "1024",
            "storage_class": "STANDARD",
            "e_tag": "abc123",
            "checksum_algorithm": "SHA256",
            "checksum_type": "FULL_OBJECT",
            "key": "path/to/file1.txt",
        },
        {
            "last_scan_timestamp": "2025-05-04T16:48:33",
            "bucket_name": "test-bucket-2",
            "last_modified": "2025-03-31T01:38:05+00:00",
            "size": "2048",
            "storage_class": "GLACIER",
            "e_tag": "def456",
            "checksum_algorithm": "SHA256",
            "checksum_type": "COMPOSITE",
            "key": "another/path/file2.txt",
        },
    ]


def test_s3_object_result_default_values():
    """Test that S3ObjectResult has proper default values."""
    result = S3ObjectResult()
    assert result.last_scan_timestamp == "MISSING"
    assert result.bucket_name == "MISSING"
    assert result.last_modified == "MISSING"
    assert result.size == "MISSING"
    assert result.storage_class == "MISSING"
    assert result.e_tag == "MISSING"
    assert result.checksum_algorithm == "MISSING"
    assert result.checksum_type == "MISSING"
    assert result.key == "MISSING"


def test_s3_object_result_custom_values():
    """Test that S3ObjectResult can be initialized with custom values."""
    result = S3ObjectResult(
        bucket_name="test-bucket",
        key="test/file.txt",
        size="1024",
    )
    assert result.bucket_name == "test-bucket"
    assert result.key == "test/file.txt"
    assert result.size == "1024"
    # Other fields should retain default values
    assert result.last_scan_timestamp == "MISSING"


def test_build_schema(tmp_path):
    """Test that build_schema returns a proper schema."""
    schema = build_schema()
    assert schema is not None
    assert isinstance(schema, tantivy.Schema)
    # Test that we can create an index with the schema
    create_index(schema, tmp_path)


def test_create_index(tmp_path):
    """Test that create_index creates an index at the specified path."""
    schema = build_schema()
    index = create_index(schema, tmp_path)
    assert index is not None
    assert tmp_path.exists()


def test_regenerate_index(tmp_path, sample_documents):
    """Test that regenerate_index creates an index with documents."""
    regenerate_index(tmp_path, sample_documents)
    assert tmp_path.exists()

    # Verify we can query the index
    results = list(run_query(tmp_path, "file1", max_results=10))
    assert len(results) == 1
    score, result = results[0]
    assert result.bucket_name == "test-bucket-1"
    assert result.key == "path/to/file1.txt"


def test_run_query(tmp_path, sample_documents):
    """Test that run_query returns expected results."""
    regenerate_index(tmp_path, sample_documents)

    # Test query for specific file
    results = list(run_query(tmp_path, "file1", max_results=10))
    assert len(results) == 1
    score, result = results[0]
    assert isinstance(score, float)
    assert result.bucket_name == "test-bucket-1"
    assert result.key == "path/to/file1.txt"
    assert result.size == "1024"

    # Test query that matches multiple files
    results = list(run_query(tmp_path, "txt", max_results=10))
    assert len(results) == 2

    # Test query with no matches
    results = list(run_query(tmp_path, "nonexistent", max_results=10))
    assert len(results) == 0


def test_run_query_max_results(tmp_path, sample_documents):
    """Test that run_query respects max_results parameter."""
    regenerate_index(tmp_path, sample_documents)

    results = list(run_query(tmp_path, "txt", max_results=1))
    assert len(results) == 1


def test_search_index_simple(tmp_path, sample_documents, capsys):
    """Test search_index_simple output format."""
    regenerate_index(tmp_path, sample_documents)

    # Test with uri_only=False
    search_index_simple(tmp_path, "file1", uri_only=False, max_results=10)
    captured = capsys.readouterr()
    output_lines = captured.out.strip().split('\n')
    assert len(output_lines) == 1
    assert "s3://test-bucket-1/path/to/file1.txt" in output_lines[0]
    assert "1024" in output_lines[0]
    assert "STANDARD" in output_lines[0]

    # Test with uri_only=True
    search_index_simple(tmp_path, "file2", uri_only=True, max_results=10)
    captured = capsys.readouterr()
    output_lines = captured.out.strip().split('\n')
    assert len(output_lines) == 1
    assert output_lines[0] == "s3://test-bucket-2/another/path/file2.txt"


def test_index_catalog(tmp_path, simple_catalog_path):
    """Test that index_catalog creates an index from a catalog."""
    from shutil import copytree

    # Create a copy of the catalog to avoid modifying test resources
    catalog_copy = tmp_path / "catalog"
    copytree(simple_catalog_path, catalog_copy)

    index_path = tmp_path / "index"
    index_catalog(catalog_copy, index_path)
    assert index_path.exists()

    # Test that we can query the indexed data
    results = list(run_query(index_path, "fastq", max_results=100))
    # Should find results based on the simple_catalog test data
    assert len(results) > 0

    # Verify some expected content from the simple catalog
    found_bucket_names = {result.bucket_name for _, result in results}
    expected_buckets = {"hgsc-a-1-2-3", "hgsc-c-123", "hgsc-d", "hgsc-b123"}
    assert found_bucket_names.intersection(expected_buckets)


def test_regenerate_index_removes_existing(tmp_path, sample_documents):
    """Test that regenerate_index removes existing index before creating new one."""
    # Create initial index
    regenerate_index(tmp_path, sample_documents)
    initial_results = list(run_query(tmp_path, "file1", max_results=10))
    assert len(initial_results) == 1

    # Create new index with different data
    new_documents = [
        {
            "last_scan_timestamp": "2025-05-05T16:48:32",
            "bucket_name": "new-bucket",
            "last_modified": "2025-04-01T01:37:05+00:00",
            "size": "512",
            "storage_class": "STANDARD",
            "e_tag": "xyz789",
            "checksum_algorithm": "SHA256",
            "checksum_type": "FULL_OBJECT",
            "key": "new/file.txt",
        }
    ]

    regenerate_index(tmp_path, new_documents)

    # Old data should be gone
    old_results = list(run_query(tmp_path, "file1", max_results=10))
    assert len(old_results) == 0

    # New data should be present
    new_results = list(run_query(tmp_path, "new", max_results=10))
    assert len(new_results) == 1
    assert new_results[0][1].bucket_name == "new-bucket"


def test_run_query_with_missing_fields(tmp_path):
    """Test run_query handles documents with missing fields gracefully."""
    # Document with some missing fields
    incomplete_documents = [
        {
            "bucket_name": "test-bucket",
            "key": "test/file.txt",
            # Missing other fields
        }
    ]

    regenerate_index(tmp_path, incomplete_documents)
    results = list(run_query(tmp_path, "file", max_results=10))

    assert len(results) == 1
    score, result = results[0]
    assert result.bucket_name == "test-bucket"
    assert result.key == "test/file.txt"
    # Missing fields should have default values
    assert result.size == "MISSING"
    assert result.storage_class == "MISSING"


def test_managed_json_permissions(tmp_path):
    """Test that .managed.json file has proper read permissions after index creation."""
    import stat

    # Create an index
    sample_documents = [
        {
            "bucket_name": "test-bucket",
            "key": "test/file.txt",
            "size": "1024",
            "storage_class": "STANDARD",
        }
    ]

    regenerate_index(tmp_path, sample_documents)

    # Check that .managed.json exists and has proper permissions
    managed_json_path = tmp_path / ".managed.json"
    assert managed_json_path.exists(), ".managed.json should exist after index creation"

    # Get file permissions
    file_stat = managed_json_path.stat()
    file_permissions = stat.filemode(file_stat.st_mode)

    # Check that the file is readable by all (at least r--r--r--)
    readable_by_owner = file_stat.st_mode & stat.S_IRUSR
    readable_by_group = file_stat.st_mode & stat.S_IRGRP
    readable_by_other = file_stat.st_mode & stat.S_IROTH

    assert readable_by_owner, (
        f".managed.json should be readable by owner, got: {file_permissions}"
    )
    assert readable_by_group, (
        f".managed.json should be readable by group, got: {file_permissions}"
    )
    assert readable_by_other, (
        f".managed.json should be readable by others, got: {file_permissions}"
    )


def test_tantivy_meta_lock_permissions(tmp_path):
    """Test that .tantivy-meta.lock file has proper write permissions."""
    import stat

    import tantivy

    # Create an index
    sample_documents = [
        {
            "bucket_name": "test-bucket",
            "key": "test/file.txt",
            "size": "1024",
            "storage_class": "STANDARD",
        }
    ]

    regenerate_index(tmp_path, sample_documents)

    # Create a searcher to trigger meta lock file creation
    schema = build_schema()
    index = tantivy.Index(schema, str(tmp_path))
    index.searcher()  # Create searcher to trigger lock file creation

    # Check that .tantivy-meta.lock exists and has proper permissions
    meta_lock_path = tmp_path / ".tantivy-meta.lock"
    if meta_lock_path.exists():
        file_stat = meta_lock_path.stat()
        file_permissions = stat.filemode(file_stat.st_mode)

        # Check that the file is writable by all (rw-rw-rw-)
        writable_by_owner = file_stat.st_mode & stat.S_IWUSR
        writable_by_group = file_stat.st_mode & stat.S_IWGRP
        writable_by_other = file_stat.st_mode & stat.S_IWOTH

        assert writable_by_owner, (
            f".tantivy-meta.lock should be writable by owner, got: {file_permissions}"
        )
        assert writable_by_group, (
            f".tantivy-meta.lock should be writable by group, got: {file_permissions}"
        )
        assert writable_by_other, (
            f".tantivy-meta.lock writable by others, got: {file_permissions}"
        )


def test_meta_json_permissions(tmp_path):
    """Test that meta.json file has proper read permissions after index creation."""
    import stat

    # Create an index
    sample_documents = [
        {
            "bucket_name": "test-bucket",
            "key": "test/file.txt",
            "size": "1024",
            "storage_class": "STANDARD",
        }
    ]

    regenerate_index(tmp_path, sample_documents)

    # Check that meta.json exists and has proper permissions
    meta_json_path = tmp_path / "meta.json"
    assert meta_json_path.exists(), "meta.json should exist after index creation"

    # Get file permissions
    file_stat = meta_json_path.stat()
    file_permissions = stat.filemode(file_stat.st_mode)

    # Check that the file is readable by all (at least r--r--r--)
    readable_by_owner = file_stat.st_mode & stat.S_IRUSR
    readable_by_group = file_stat.st_mode & stat.S_IRGRP
    readable_by_other = file_stat.st_mode & stat.S_IROTH

    assert readable_by_owner, (
        f"meta.json should be readable by owner, got: {file_permissions}"
    )
    assert readable_by_group, (
        f"meta.json should be readable by group, got: {file_permissions}"
    )
    assert readable_by_other, (
        f"meta.json should be readable by others, got: {file_permissions}"
    )

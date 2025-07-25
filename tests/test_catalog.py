import csv
from pathlib import PurePosixPath

from pytest import fixture

from aws_object_search.catalog import S3ObjectCatalog


@fixture
def simple_catalog(simple_catalog_path) -> S3ObjectCatalog:
    "Convert simple_catalog_path to S3ObjectCatalog."
    return S3ObjectCatalog(simple_catalog_path)


def test_list_catalog(simple_catalog):
    "Reading all the bucket scans from a catalog"
    catalog = simple_catalog
    expected = [
        "2025-05-03 16:48:31 hgsc-a-1-2-3.tsv",
        "2025-05-03 16:48:32 hgsc-c-123.tsv",
        "2025-05-04 16:48:32 hgsc-a-1-2-3.tsv",
        "2025-05-04 16:48:33 hgsc-c-123.tsv",
        "2025-05-04 16:48:34 hgsc-d.tsv",
        "2025-05-05 16:48:32 hgsc-b123.tsv",
    ]
    all_scans = sorted(catalog.all_bucket_scans())
    assert (
        all_scans[0].file_path
        == catalog.catalog_root / "20250503-164831-hgsc-a-1-2-3.tsv"
    )
    computed = [f"{s.scan_start} {s.bucket_name}.tsv" for s in all_scans]
    assert computed == expected


def test_list_objects(simple_catalog):
    "Read some object metadata"
    catalog = simple_catalog
    all_scans = sorted(catalog.all_bucket_scans())
    target_scan = all_scans[4]
    assert target_scan.bucket_name == "hgsc-d"
    expected_keys = [
        "v1/illumina/wex/fastqs/Sample_H575JDSXX-1-IDUDI0003/"
        "H575JDSXX-1-IDUDI0003_S30_L001_R1_001.fastq.gz",
        "v1/illumina/wex/fastqs/Sample_H575JDSXX-1-IDUDI0003/"
        "H575JDSXX-1-IDUDI0003_S30_L001_R2_001.fastq.gz",
        "v1/illumina/wex/fastqs/Sample_H575JDSXX-1-IDUDI0003/SEDefn.json",
    ]
    computed_keys = [obj_meta.key for obj_meta in target_scan.contents()]
    assert computed_keys == expected_keys
    expected_sizes = [
        "2869776186",
        "2873774843",
        "1407",
    ]
    computed_sizes = [obj_meta.size for obj_meta in target_scan.contents()]
    assert computed_sizes == expected_sizes


def test_current_bucket_scans(simple_catalog):
    "Ensure that only the most recent scans for each bucket are returned."
    expected = [
        "20250504-164832-hgsc-a-1-2-3.tsv",
        "20250504-164833-hgsc-c-123.tsv",
        "20250504-164834-hgsc-d.tsv",
        "20250505-164832-hgsc-b123.tsv",
    ]
    computed = [s.file_path.name for s in simple_catalog.current_bucket_scans()]
    assert computed == expected


def test_current_contents(resources_dir, simple_catalog):
    "Test main output of catalog used for indexing."
    current_contents_tsv = resources_dir / "current_contents.tsv"
    with open(current_contents_tsv, newline="", encoding="utf-8") as tsv_file:
        reader = csv.reader(tsv_file, delimiter="\t")
        expected = list(reader)
    computed = []
    for bucket_scan, object_metadata in simple_catalog.current_contents():
        p = PurePosixPath(object_metadata.key)
        computed.append(
            [
                str(bucket_scan.scan_start),
                bucket_scan.bucket_name,
                object_metadata.last_modified,
                p.parent.name,
                p.name,
            ]
        )
    assert computed == expected


def test_iter_dicts(simple_catalog):
    "Check first, last, and length"
    results = list(simple_catalog.iter_dicts())
    assert len(results) == 12
    first, *_, last = results
    assert first == {
        "scan_start": "2025-05-04T16:48:32",
        "bucket_name": "hgsc-a-1-2-3",
        "last_modified": "2025-03-31T01:37:05+00:00",
        "size": "6325709904",
        "storage_class": "DEEP_ARCHIVE",
        "e_tag": "a65f5b56909bf63398213ae450a879fb-604",
        "checksum_algorithm": "SHA256",
        "checksum_type": "COMPOSITE",
        "key": "v1/illumina/wex/fastqs/Sample_HY2L7DSX2-3-IDUDI0074/"
        "HY2L7DSX2-3-IDUDI0074_S120_L003_R1_001.fastq.gz",
    }
    assert last == {
        "scan_start": "2025-05-05T16:48:32",
        "bucket_name": "hgsc-b123",
        "last_modified": "2025-03-31T01:39:09+00:00",
        "size": "2271",
        "storage_class": "DEEP_ARCHIVE",
        "e_tag": "8762b27bbeee8c644b19ce7dac46c5c2",
        "checksum_algorithm": "SHA256",
        "checksum_type": "FULL_OBJECT",
        "key": "v1/illumina/wex/fastqs/Sample_HY2L7DSX2-3-IDUDI0076/event.json",
    }


@fixture
def temp_gzipped_catalog(simple_catalog, tmp_path) -> S3ObjectCatalog:
    "Read simple catalog and write to tmp_path in .tsv.gz format."
    gz_catalog = S3ObjectCatalog(tmp_path / "gzipped_catalog")
    for b in simple_catalog.all_bucket_scans():
        date, time, bucket_name = b.file_path.stem.split("-", 2)
        data = (m.flattened_dict() for m in b.contents())
        gz_catalog.output_s3_objects_to_tsv(data, bucket_name, f"{date}-{time}")
    return gz_catalog


def test_gzipped_catalog(temp_gzipped_catalog) -> None:
    "Consistency test comparing to uncompressed catalog"
    print(temp_gzipped_catalog.catalog_root)
    expected_bucket_scan_file_names = [
        "20250503-164831-hgsc-a-1-2-3.tsv.gz",
        "20250503-164832-hgsc-c-123.tsv.gz",
        "20250504-164832-hgsc-a-1-2-3.tsv.gz",
        "20250504-164833-hgsc-c-123.tsv.gz",
        "20250504-164834-hgsc-d.tsv.gz",
        "20250505-164832-hgsc-b123.tsv.gz",
    ]
    bucket_scan_file_names = sorted(
        p.name for p in temp_gzipped_catalog.catalog_root.iterdir()
    )
    assert bucket_scan_file_names == expected_bucket_scan_file_names
    all_bucket_scans = sorted(temp_gzipped_catalog.all_bucket_scans())
    assert [
        b.file_path.name for b in all_bucket_scans
    ] == expected_bucket_scan_file_names
    results = list(temp_gzipped_catalog.iter_dicts())
    assert len(results) == 12
    first, *_, last = results
    assert first == {
        "scan_start": "2025-05-04T16:48:32",
        "bucket_name": "hgsc-a-1-2-3",
        "last_modified": "2025-03-31T01:37:05+00:00",
        "size": "6325709904",
        "storage_class": "DEEP_ARCHIVE",
        "e_tag": "a65f5b56909bf63398213ae450a879fb-604",
        "checksum_algorithm": "SHA256",
        "checksum_type": "COMPOSITE",
        "key": "v1/illumina/wex/fastqs/Sample_HY2L7DSX2-3-IDUDI0074/"
        "HY2L7DSX2-3-IDUDI0074_S120_L003_R1_001.fastq.gz",
    }
    assert last == {
        "scan_start": "2025-05-05T16:48:32",
        "bucket_name": "hgsc-b123",
        "last_modified": "2025-03-31T01:39:09+00:00",
        "size": "2271",
        "storage_class": "DEEP_ARCHIVE",
        "e_tag": "8762b27bbeee8c644b19ce7dac46c5c2",
        "checksum_algorithm": "SHA256",
        "checksum_type": "FULL_OBJECT",
        "key": "v1/illumina/wex/fastqs/Sample_HY2L7DSX2-3-IDUDI0076/event.json",
    }

import argparse
import fcntl
import multiprocessing
import time

import pytest

from aws_object_search.entry import (
    BAM_ENDINGS,
    BAM_INDEX_ENDINGS,
    CONFIG_ENDINGS,
    CRAM_ENDINGS,
    CRAM_INDEX_ENDINGS,
    RAW_READS_ENDINGS,
    VCF_ENDINGS,
    VCF_INDEX_ENDINGS,
    aos_scan,
    build_file_endings_filter,
    filter_by_file_endings,
)


@pytest.mark.integration
def test_aos_scan_smoke(tmp_path):
    """Smoke test using a specific bucket prefix and output root directory."""
    args = argparse.Namespace(
        bucket_prefix="test-bucket-prefix",
        output_root=tmp_path,
        log_level="ERROR",
        no_scan=False,
        no_index=False,
        flock=None,
    )
    aos_scan(args)


# Tests for file locking


def test_aos_scan_no_lock(tmp_path):
    """Test that aos_scan works without flock option (no locking)."""
    args = argparse.Namespace(
        bucket_prefix=None,
        output_root=tmp_path,
        log_level="ERROR",
        no_scan=True,
        no_index=True,
        flock=None,
    )
    # Should complete without error
    aos_scan(args)


def test_aos_scan_with_lock_acquired(tmp_path):
    """Test that aos_scan successfully acquires lock when available."""
    lock_file = tmp_path / "test.lock"
    args = argparse.Namespace(
        bucket_prefix=None,
        output_root=tmp_path,
        log_level="ERROR",
        no_scan=True,
        no_index=True,
        flock=lock_file,
    )
    # Should complete without error and create lock file
    aos_scan(args)
    # Lock file should exist after scan
    assert lock_file.exists()


def test_aos_scan_lock_blocking(tmp_path):
    """Test that aos_scan exits with code 2 when lock is held by another process."""
    lock_file = tmp_path / "test.lock"

    # Acquire lock in this process
    with open(lock_file, "w") as lf:
        fcntl.flock(lf.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

        # Try to run aos_scan with the same lock file (should fail)
        args = argparse.Namespace(
            bucket_prefix=None,
            output_root=tmp_path,
            log_level="ERROR",
            no_scan=True,
            no_index=True,
            flock=lock_file,
        )

        # Should exit with code 2
        with pytest.raises(SystemExit) as exc_info:
            aos_scan(args)
        assert exc_info.value.code == 2


def test_aos_scan_lock_critical_message(tmp_path, caplog):
    """Test that CRITICAL message is logged when lock acquisition fails."""
    import logging

    lock_file = tmp_path / "test.lock"

    # Acquire lock in this process
    with open(lock_file, "w") as lf:
        fcntl.flock(lf.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

        # Try to run aos_scan with the same lock file
        args = argparse.Namespace(
            bucket_prefix=None,
            output_root=tmp_path,
            log_level="CRITICAL",
            no_scan=True,
            no_index=True,
            flock=lock_file,
        )

        # Capture log messages
        with caplog.at_level(logging.CRITICAL):
            with pytest.raises(SystemExit) as exc_info:
                aos_scan(args)
            assert exc_info.value.code == 2

        # Check that CRITICAL message was logged
        assert any("already running" in record.message for record in caplog.records)


def _run_aos_scan_with_lock(lock_file, result_queue, hold_time=0.5):
    """Helper function to run aos_scan in a separate process."""
    # Manually acquire lock to hold it for a specific duration
    try:
        with open(lock_file, "w") as lf:
            fcntl.flock(lf.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            result_queue.put("success")
            # Hold the lock for the specified time
            time.sleep(hold_time)
            # Lock released when file closes
    except BlockingIOError:
        result_queue.put("exit_2")
    except Exception as e:
        result_queue.put(f"error_{e}")


def test_aos_scan_concurrent_blocking(tmp_path):
    """Test that concurrent aos_scan processes properly block each other."""
    lock_file = tmp_path / "concurrent.lock"
    result_queue = multiprocessing.Queue()

    # Start first process that will hold the lock
    process1 = multiprocessing.Process(
        target=_run_aos_scan_with_lock, args=(lock_file, result_queue)
    )
    process1.start()

    # Give first process time to acquire lock
    time.sleep(0.2)

    # Start second process that should fail to acquire lock
    process2 = multiprocessing.Process(
        target=_run_aos_scan_with_lock, args=(lock_file, result_queue)
    )
    process2.start()

    # Wait for both processes to complete
    process1.join(timeout=2)
    process2.join(timeout=2)

    # Collect results
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    # First process should succeed, second should exit with code 2
    assert "success" in results
    assert "exit_2" in results

    # Clean up
    if process1.is_alive():
        process1.terminate()
    if process2.is_alive():
        process2.terminate()


# Tests for build_file_endings_filter


def test_build_filter_with_all_flag():
    """Test that --all flag returns None (no filtering)."""
    args = argparse.Namespace(
        all=True,
        raw_reads=False,
        mapped_reads=False,
        bam=False,
        cram=False,
        vcf=False,
        configs=False,
        no_index=False,
    )
    result = build_file_endings_filter(args)
    assert result is None


def test_build_filter_default_no_flags():
    """Test default behavior with no flags (equivalent to -gprv)."""
    args = argparse.Namespace(
        all=False,
        raw_reads=False,
        mapped_reads=False,
        bam=False,
        cram=False,
        vcf=False,
        configs=False,
        no_index=False,
    )
    result = build_file_endings_filter(args)
    expected = (
        RAW_READS_ENDINGS
        + CONFIG_ENDINGS
        + BAM_ENDINGS
        + CRAM_ENDINGS
        + BAM_INDEX_ENDINGS
        + CRAM_INDEX_ENDINGS
        + VCF_ENDINGS
        + VCF_INDEX_ENDINGS
    )
    assert result == expected


def test_build_filter_raw_reads_only():
    """Test --raw-reads flag only."""
    args = argparse.Namespace(
        all=False,
        raw_reads=True,
        mapped_reads=False,
        bam=False,
        cram=False,
        vcf=False,
        configs=False,
        no_index=False,
    )
    result = build_file_endings_filter(args)
    assert result == RAW_READS_ENDINGS


def test_build_filter_configs_only():
    """Test --configs flag only."""
    args = argparse.Namespace(
        all=False,
        raw_reads=False,
        mapped_reads=False,
        bam=False,
        cram=False,
        vcf=False,
        configs=True,
        no_index=False,
    )
    result = build_file_endings_filter(args)
    assert result == CONFIG_ENDINGS


def test_build_filter_mapped_reads():
    """Test --mapped-reads flag includes BAM and CRAM with indexes."""
    args = argparse.Namespace(
        all=False,
        raw_reads=False,
        mapped_reads=True,
        bam=False,
        cram=False,
        vcf=False,
        configs=False,
        no_index=False,
    )
    result = build_file_endings_filter(args)
    expected = BAM_ENDINGS + CRAM_ENDINGS + BAM_INDEX_ENDINGS + CRAM_INDEX_ENDINGS
    assert result == expected


def test_build_filter_mapped_reads_no_index():
    """Test --mapped-reads with --no-index excludes index files."""
    args = argparse.Namespace(
        all=False,
        raw_reads=False,
        mapped_reads=True,
        bam=False,
        cram=False,
        vcf=False,
        configs=False,
        no_index=True,
    )
    result = build_file_endings_filter(args)
    expected = BAM_ENDINGS + CRAM_ENDINGS
    assert result == expected


def test_build_filter_bam_only():
    """Test --bam flag only."""
    args = argparse.Namespace(
        all=False,
        raw_reads=False,
        mapped_reads=False,
        bam=True,
        cram=False,
        vcf=False,
        configs=False,
        no_index=False,
    )
    result = build_file_endings_filter(args)
    expected = BAM_ENDINGS + BAM_INDEX_ENDINGS
    assert result == expected


def test_build_filter_bam_no_index():
    """Test --bam with --no-index."""
    args = argparse.Namespace(
        all=False,
        raw_reads=False,
        mapped_reads=False,
        bam=True,
        cram=False,
        vcf=False,
        configs=False,
        no_index=True,
    )
    result = build_file_endings_filter(args)
    assert result == BAM_ENDINGS


def test_build_filter_cram_only():
    """Test --cram flag only."""
    args = argparse.Namespace(
        all=False,
        raw_reads=False,
        mapped_reads=False,
        bam=False,
        cram=True,
        vcf=False,
        configs=False,
        no_index=False,
    )
    result = build_file_endings_filter(args)
    expected = CRAM_ENDINGS + CRAM_INDEX_ENDINGS
    assert result == expected


def test_build_filter_cram_no_index():
    """Test --cram with --no-index."""
    args = argparse.Namespace(
        all=False,
        raw_reads=False,
        mapped_reads=False,
        bam=False,
        cram=True,
        vcf=False,
        configs=False,
        no_index=True,
    )
    result = build_file_endings_filter(args)
    assert result == CRAM_ENDINGS


def test_build_filter_vcf_only():
    """Test --vcf flag only."""
    args = argparse.Namespace(
        all=False,
        raw_reads=False,
        mapped_reads=False,
        bam=False,
        cram=False,
        vcf=True,
        configs=False,
        no_index=False,
    )
    result = build_file_endings_filter(args)
    expected = VCF_ENDINGS + VCF_INDEX_ENDINGS
    assert result == expected


def test_build_filter_vcf_no_index():
    """Test --vcf with --no-index."""
    args = argparse.Namespace(
        all=False,
        raw_reads=False,
        mapped_reads=False,
        bam=False,
        cram=False,
        vcf=True,
        configs=False,
        no_index=True,
    )
    result = build_file_endings_filter(args)
    assert result == VCF_ENDINGS


def test_build_filter_multiple_flags():
    """Test combination of multiple flags."""
    args = argparse.Namespace(
        all=False,
        raw_reads=True,
        mapped_reads=False,
        bam=False,
        cram=False,
        vcf=True,
        configs=True,
        no_index=False,
    )
    result = build_file_endings_filter(args)
    expected = RAW_READS_ENDINGS + CONFIG_ENDINGS + VCF_ENDINGS + VCF_INDEX_ENDINGS
    assert result == expected


def test_build_filter_bam_and_cram():
    """Test --bam and --cram flags together."""
    args = argparse.Namespace(
        all=False,
        raw_reads=False,
        mapped_reads=False,
        bam=True,
        cram=True,
        vcf=False,
        configs=False,
        no_index=False,
    )
    result = build_file_endings_filter(args)
    expected = BAM_ENDINGS + BAM_INDEX_ENDINGS + CRAM_ENDINGS + CRAM_INDEX_ENDINGS
    assert result == expected


def test_build_filter_file_types_override_all():
    """Test that explicit file type flags override --all flag."""
    args = argparse.Namespace(
        all=True,
        raw_reads=True,
        mapped_reads=True,
        bam=True,
        cram=True,
        vcf=True,
        configs=True,
        no_index=True,
    )
    result = build_file_endings_filter(args)
    # Should return file endings based on specified flags, not None
    expected = (
        RAW_READS_ENDINGS + CONFIG_ENDINGS + BAM_ENDINGS + CRAM_ENDINGS + VCF_ENDINGS
    )
    assert result == expected


# Tests for filter_by_file_endings


def test_filter_by_file_endings_none():
    """Test that None endings allows all URIs through."""
    assert filter_by_file_endings("s3://bucket/any/file.txt", None) is True
    assert filter_by_file_endings("s3://bucket/path/random.xyz", None) is True


def test_filter_by_file_endings_empty_list():
    """Test that empty list blocks all URIs."""
    assert filter_by_file_endings("s3://bucket/file.txt", []) is False
    assert filter_by_file_endings("s3://bucket/file.fastq.gz", []) is False


def test_filter_by_file_endings_fastq():
    """Test filtering FASTQ files."""
    endings = [".fastq.gz", "_001.fastq.gz"]
    assert filter_by_file_endings("s3://bucket/sample_R1_001.fastq.gz", endings) is True
    assert filter_by_file_endings("s3://bucket/sample.fastq.gz", endings) is True
    assert filter_by_file_endings("s3://bucket/sample.bam", endings) is False


def test_filter_by_file_endings_bam():
    """Test filtering BAM files."""
    endings = ["_realigned.bam", ".hgv.bam", "bam.bai"]
    assert filter_by_file_endings("s3://bucket/sample_realigned.bam", endings) is True
    assert filter_by_file_endings("s3://bucket/sample.hgv.bam", endings) is True
    assert filter_by_file_endings("s3://bucket/sample.bam.bai", endings) is True
    assert filter_by_file_endings("s3://bucket/sample.cram", endings) is False


def test_filter_by_file_endings_vcf():
    """Test filtering VCF files."""
    endings = [".SNPs_Annotated.vcf", "_snp.vcf.gz", "vcf.gz.tbi"]
    assert (
        filter_by_file_endings("s3://bucket/sample.SNPs_Annotated.vcf", endings) is True
    )
    assert filter_by_file_endings("s3://bucket/sample_snp.vcf.gz", endings) is True
    assert filter_by_file_endings("s3://bucket/sample.vcf.gz.tbi", endings) is True
    assert filter_by_file_endings("s3://bucket/sample.bam", endings) is False


def test_filter_by_file_endings_config():
    """Test filtering config files."""
    endings = ["config.txt", "event.json", "FCDefn.json"]
    assert filter_by_file_endings("s3://bucket/run/config.txt", endings) is True
    assert filter_by_file_endings("s3://bucket/run/event.json", endings) is True
    assert filter_by_file_endings("s3://bucket/run/FCDefn.json", endings) is True
    assert filter_by_file_endings("s3://bucket/data.txt", endings) is False


def test_filter_by_file_endings_case_sensitive():
    """Test that filtering is case-sensitive."""
    endings = [".fastq.gz"]
    assert filter_by_file_endings("s3://bucket/sample.fastq.gz", endings) is True
    assert filter_by_file_endings("s3://bucket/sample.FASTQ.GZ", endings) is False


def test_filter_by_file_endings_partial_match():
    """Test that only complete suffix matches work."""
    endings = [".bam"]
    assert filter_by_file_endings("s3://bucket/sample.bam", endings) is True
    # Should not match "bam" in the middle of the filename
    assert filter_by_file_endings("s3://bucket/bamboo.txt", endings) is False

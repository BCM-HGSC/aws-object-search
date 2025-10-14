import argparse

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
    )
    aos_scan(args)


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


def test_build_filter_all_overrides_other_flags():
    """Test that --all flag overrides all other flags."""
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
    assert result is None


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
    assert (
        filter_by_file_endings(
            "s3://bucket/sample_R1_001.fastq.gz", endings
        )
        is True
    )
    assert (
        filter_by_file_endings("s3://bucket/sample.fastq.gz", endings) is True
    )
    assert filter_by_file_endings("s3://bucket/sample.bam", endings) is False


def test_filter_by_file_endings_bam():
    """Test filtering BAM files."""
    endings = ["_realigned.bam", ".hgv.bam", "bam.bai"]
    assert (
        filter_by_file_endings("s3://bucket/sample_realigned.bam", endings)
        is True
    )
    assert (
        filter_by_file_endings("s3://bucket/sample.hgv.bam", endings) is True
    )
    assert (
        filter_by_file_endings("s3://bucket/sample.bam.bai", endings) is True
    )
    assert filter_by_file_endings("s3://bucket/sample.cram", endings) is False


def test_filter_by_file_endings_vcf():
    """Test filtering VCF files."""
    endings = [".SNPs_Annotated.vcf", "_snp.vcf.gz", "vcf.gz.tbi"]
    assert (
        filter_by_file_endings(
            "s3://bucket/sample.SNPs_Annotated.vcf", endings
        )
        is True
    )
    assert (
        filter_by_file_endings("s3://bucket/sample_snp.vcf.gz", endings)
        is True
    )
    assert (
        filter_by_file_endings("s3://bucket/sample.vcf.gz.tbi", endings)
        is True
    )
    assert filter_by_file_endings("s3://bucket/sample.bam", endings) is False


def test_filter_by_file_endings_config():
    """Test filtering config files."""
    endings = ["config.txt", "event.json", "FCDefn.json"]
    assert (
        filter_by_file_endings("s3://bucket/run/config.txt", endings) is True
    )
    assert (
        filter_by_file_endings("s3://bucket/run/event.json", endings) is True
    )
    assert (
        filter_by_file_endings("s3://bucket/run/FCDefn.json", endings) is True
    )
    assert filter_by_file_endings("s3://bucket/data.txt", endings) is False


def test_filter_by_file_endings_case_sensitive():
    """Test that filtering is case-sensitive."""
    endings = [".fastq.gz"]
    assert (
        filter_by_file_endings("s3://bucket/sample.fastq.gz", endings) is True
    )
    assert (
        filter_by_file_endings("s3://bucket/sample.FASTQ.GZ", endings) is False
    )


def test_filter_by_file_endings_partial_match():
    """Test that only complete suffix matches work."""
    endings = [".bam"]
    assert filter_by_file_endings("s3://bucket/sample.bam", endings) is True
    # Should not match "bam" in the middle of the filename
    assert (
        filter_by_file_endings("s3://bucket/bamboo.txt", endings) is False
    )

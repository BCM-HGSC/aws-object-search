# AWS Object Search

This documentation describes how to search for files in AWS S3 buckets using the AWS Object Search tool.

## Prerequisites

You must have:

- Terminal access
- The `search-aws` and `search.py` aliases available in your shell (these should not be suppressed)
- Understanding of the following terms:
  - [S3 bucket](https://en.wikipedia.org/wiki/Amazon_S3): Cloud storage container in AWS
  - [TSV file](https://en.wikipedia.org/wiki/Tab-separated_values): Text file with data separated by tabs
  - [Compressed file](https://en.wikipedia.org/wiki/Data_compression): File that has been reduced in size using compression algorithms (e.g., `.gz`, `.bz2`)

## Search Commands Overview

The AWS Object Search tool provides two search commands:

### `search-aws`
Single search query executed immediately with results printed to the terminal.

**Basic usage:**
```bash
search-aws "search_term"
```

### `search.py`
Batch search that processes multiple queries from a text file and generates organized output files.

**Basic usage:**
```bash
search.py input_file.txt
```

## Basic Examples

### Single Search: Find Files by Name

Search for files containing a specific term:

```bash
search-aws "sample_001"
```

### Batch Search: Process Multiple Queries

Create a text file with one search term per line:

```bash
cat > sample_list.txt << EOF
sample_001
sample_002
sample_003
EOF
```

Then process all searches at once:

```bash
search.py sample_list.txt
```

This creates several output files:
- `sample_list.txt.out.tsv`: All matching files (tab-separated format with metadata)
- `sample_list.txt.out.info`: Summary showing match counts
- `sample_list.txt.not_found.txt`: Detailed information about terms with no matches
- `sample_list.txt.not_found.list`: Simple list of terms that had no matches

## Intermediate Examples

### Filter Results by File Type

By default, results show configuration files, raw reads, mapped reads, and VCF files. You can filter to show only specific file types:

```bash
# Show only mapped reads (BAM and CRAM files)
search-aws "project_name" -p

# Show only raw reads (FASTQ files)
search-aws "project_name" -r

# Show only VCF files
search-aws "project_name" -v

# Show configuration files
search-aws "project_name" -g

# Show all files without filtering
search-aws "project_name" -a
```

File type flags can be combined:

```bash
# Show FASTQ and VCF files together
search.py sample_list.txt -r -v
```

### Clean Output Format

By default, results include metadata (size, modification date, storage class). For a clean list of S3 URIs only:

```bash
search-aws "sample_xyz" -u
```

### Limit Number of Results

By default, the tool returns up to 10,000,000 results. To limit the output:

```bash
# Return only first 1000 matches
search-aws "project_name" -m 1000
```

## Advanced Examples

### Direct TSV File Access

For advanced users who need to analyze raw catalog data, the S3 objects directory contains compressed TSV files with complete metadata:

```bash
/hgsc_software/aws-object-search/s3_objects
```

These files contain bucket inventory data. To search them directly, remember:

- Files ending in `.gz` are gzip-compressed. Use `zgrep` instead of `grep`:

```bash
zgrep "search_pattern" /hgsc_software/aws-object-search/s3_objects/catalog.tsv.gz
```

- Alternatively, decompress explicitly before searching:

```bash
zcat /hgsc_software/aws-object-search/s3_objects/catalog.tsv.gz | grep "search_pattern"
```

The `s3_objects/index/` directory contains a Tantivy search index that powers the `search-aws` and `search.py` commands. Unless you are writing a custom query tool, there is no need to access the files in this directory directly.

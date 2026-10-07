# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Setup

The development environment is a standard uv project (`.venv` in the repo root):
```bash
uv sync --extra dev
uv run pre-commit install --hook-type pre-commit --hook-type commit-msg
```

Developers are expected to have `uv` on their `PATH`. Run `uv lock` whenever `pyproject.toml` changes and commit `uv.lock` with it.

`./deploy` is the production installer (micromamba + its own uv, versioned `aws-object-search-SUFFIX` directory). Do not use it for development; to test the production layout, deploy into a scratch prefix with `./deploy -p SCRATCH_DIR SUFFIX`.

For AWS operations, ensure you have:
```bash
export AWS_PROFILE=scan-dev  # or appropriate profile
aws sso login
```

## Common Commands

### Testing
```bash
# Run all tests (integration tests are skipped)
uv run pytest

# Run specific test file
uv run pytest tests/test_catalog.py

# Also run integration tests (@pytest.mark.integration; need AWS)
uv run pytest --run-integration
```

### Running the Tools in Development
The default output root is `s3_objects/` beside the environment, i.e. in the repo root (gitignored).

```bash
# Scan S3 buckets with prefix
uv run aos-scan --bucket-prefix hgsc-b

# Scan with file locking to prevent concurrent scans
uv run aos-scan --bucket-prefix hgsc-b --flock /path/to/lock/file

# Search the index
uv run search-aws query_string
uv run search.py input_file.txt

# Search with file type filtering
uv run search-aws query_string --raw-reads  # Only FASTQ files
uv run search-aws query_string --all        # All results, no filtering
uv run search-aws query_string -gprv        # Configs, mapped-reads, raw-reads, VCF (default)

# Lint
uv run ruff check PATH/TO/FILE
```

## Architecture Overview

This is an S3 object search system with two main phases:

### Phase 0: CLI entry points
- **Entry Points** (`entry.py`): Command-line interfaces for both scan and search operations

### Phase 1: Scanning and Indexing (`aos-scan`)
- **S3 Scanner** (`s3_wrapper.py`): Lists all objects in S3 buckets, outputs to TSV files
- **Catalog Management** (`catalog.py`): Handles TSV file operations and metadata
- **Indexer** (`tantivy_wrapper.py`): Ingests TSV catalog files into a Tantivy search index

### Phase 2: Searching (`search-aws`, `search.py`)
- **Search Interface** (`tantivy_wrapper.py`): Queries the Tantivy index
- **File Type Filtering** (`entry.py`): Post-query filtering by file endings to show only relevant results

### Key Components
- `src/aws_object_search/s3_wrapper.py`: AWS S3 interaction, bucket scanning
- `src/aws_object_search/tantivy_wrapper.py`: Search index creation and querying
- `src/aws_object_search/catalog.py`: TSV catalog file management
- `src/aws_object_search/entry.py`: CLI entry points (aos-scan, search-aws, search.py)

### Data Flow
1. `aos-scan` reads S3 buckets → generates TSV files in `s3_objects/`
2. Indexer processes TSV files → creates search index in `s3_objects/index/`
3. `search-aws` and `search.py` query the index for fast search results

### Deployment Structure
Production uses versioned deployments via the `deploy` script:
- Creates `aws-object-search-VERSION` directory with micromamba environment
- Installs package with uv
- Creates `s3_objects/` output directory
- In production: symlinked as `current` for stable path reference

## Code Quality and Validation

- Always run ruff to validate new code.

## Release Management

See `RELEASING.md` for full release procedures.

The `/release VERSION` skill (`.claude/skills/release/`) automates a release: it drafts notes into `release-notes/`, waits for approval, then runs `scripts/release-finalize.sh`. Run it from a clean, up-to-date `main`.

### Git Tag Style

Annotated Git tags are the authoritative source of release notes. Tags use a structured format:

```
Release vX.Y.Z

Summary:
<1–2 sentences describing the release>

Changes:
- <bullet 1>
- <bullet 2>

Notes:
- <optional compatibility notes, deprecations>

Commit: <SHA>
Date: <ISO-8601>
```

Use the `git release` alias (defined in RELEASING.md) to create tags with a pre-filled template:

```bash
git release vX.Y.Z
```

## File Handling Guidelines

- Files should always end with a newline unless the target file format forbids it.

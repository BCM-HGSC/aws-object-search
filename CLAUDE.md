# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Setup

Deploy the software for development:
```bash
./deploy
```

This creates an `aws-object-search-dev` directory with the development environment and all dependencies already installed.

Although the `deploy` script uses `uv`, there is no need for developers to use `uv` unless they are changing entry points or dependencies.

For AWS operations, ensure you have:
```bash
export AWS_PROFILE=scan-dev  # or appropriate profile
aws sso login
```

## Common Commands

### Testing
```bash
# Run all tests
./bin/pytest

# Run specific test file
./bin/pytest tests/test_catalog.py

# Run integration tests (marked with @pytest.mark.integration)
./bin/pytest -m integration
```

### Updates in Development
This is only necessary when changing entry points or dependencies:

```bash
# Install in development mode (editable)
uv pip install --system -e ".[dev]"
```

### Running the Tools in Development
There is a symlink in the project root named `env` that points to the devault development environment at `../aws-object-search-dev`.
There is a `bin` in the project root with symlinks to executables in `env/bin/`.


```bash
# Scan S3 buckets with prefix
bin/aos-scan --bucket-prefix hgsc-b

# Search the index
bin/search-aws
bin/search.py

# Using ruff to check PATH/TO/FILE
bin/ruff check PATH/TO/FILE

# Legacy brute-force search (should never need to run)
python bin/searchGlacier.py
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

## File Handling Guidelines

- Files should always end with a newline unless the target file format forbids it.

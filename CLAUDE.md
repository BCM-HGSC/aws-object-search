# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Setup

Deploy the software for development:
```bash
./deploy
```

This creates an `aws-object-search-dev` directory with the development environment.

For AWS operations, ensure you have:
```bash
export AWS_PROFILE=prod-sub  # or appropriate profile
aws sso login
```

## Common Commands

### Testing
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_catalog.py

# Run integration tests (marked with @pytest.mark.integration)
pytest -m integration
```

### Installation/Development
```bash
# Install in development mode (editable)
uv pip install --system -e .

# Install with dev dependencies
uv pip install --system -e ".[dev]"
```

### Running the Tools
```bash
# Scan S3 buckets with prefix
./aws-object-search-dev/bin/aos-scan --bucket-prefix hgsc-b

# Search the index
./aws-object-search-dev/bin/aos-search s3_objects "search query"

# Legacy brute-force search
python bin/searchGlacier.py
```

## Architecture Overview

This is an S3 object search system with two main phases:

### Phase 1: Scanning and Indexing (`aos-scan`)
- **S3 Scanner** (`s3_wrapper.py`): Lists all objects in S3 buckets, outputs to TSV files
- **Indexer** (`tantivy_wrapper.py`): Ingests TSV catalog files into a Tantivy search index
- **Catalog Management** (`catalog.py`): Handles TSV file operations and metadata

### Phase 2: Searching (`aos-search`)
- **Search Interface** (`tantivy_wrapper.py`): Queries the Tantivy index
- **Entry Points** (`entry.py`): Command-line interfaces for both scan and search operations

### Key Components
- `src/aws_object_search/s3_wrapper.py`: AWS S3 interaction, bucket scanning
- `src/aws_object_search/tantivy_wrapper.py`: Search index creation and querying
- `src/aws_object_search/catalog.py`: TSV catalog file management
- `src/aws_object_search/entry.py`: CLI entry points (aos-scan, aos-search)

### Data Flow
1. `aos-scan` reads S3 buckets → generates TSV files in `s3_objects/`
2. Indexer processes TSV files → creates search index in `s3_objects/index/`
3. `aos-search` queries the index for fast search results

### Deployment Structure
Production uses versioned deployments via the `deploy` script:
- Creates `aws-object-search-VERSION` directory with micromamba environment
- Installs package with uv
- Creates `s3_objects/` output directory
- In production: symlinked as `current` for stable path reference
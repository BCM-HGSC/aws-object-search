# aws-object-search

This repo is a collection of tools to facilitate searching the contents of a
collection of S3 buckets.

## Development Setup

Deploy the software for development:
```bash
./deploy
```

This creates an `aws-object-search-dev` directory with the development environment and all dependencies already installed.

For AWS operations, ensure you have:
```bash
export AWS_PROFILE=scan-dev  # or appropriate profile
aws sso login
```

## Testing

```bash
# Run all tests
./bin/pytest

# Run specific test file
./bin/pytest tests/test_catalog.py

# Run integration tests (marked with @pytest.mark.integration)
./bin/pytest -m integration
```

## Tools

- `search.py`: Main search interface that reads a text file containing search terms and generates output files
- `search-aws`: Command-line search tool that accepts a single search term and outputs to stdout
- `aos-scan`: Scans S3 buckets (all or with specified prefix) and generates TSV catalog files, then creates search index
- `python3 bin/searchGlacier.py`: Legacy brute-force `O(N)` metadata search of all objects within readable or specified buckets. This script is mainly for reference and should not normally be used. Unless the buckets to be searched are limited, a single search could take several minutes.

### Running Tools in Development

There is a `bin/` directory in the project root with symlinks to executables:

```bash
# Scan S3 buckets with prefix
bin/aos-scan --bucket-prefix hgsc-b

# Search the index
bin/search-aws "search_term"
bin/search.py input_file.txt

# Code quality validation
bin/ruff check PATH/TO/FILE
```

## Technical Architecture

This is very similar to how the tape archive search system is built, but the scanning and indexing use different technologies, and the search engine is tantivy instead of Solr.
The idea is to create text files that are catalogs of the archive, load those text files into a search engine (index), and then run queries against that search engine.

### components

- updater (aos-scan)
    - scanner
    - indexer
- searcher
    - search.py
    - search-aws

## execution overview

1. Running under a privileged account managed by the SysAdmin team, a cron job runs aos-scan  to scan buckets and generates compressed text files on local storage. Although the credentials used by aos-scan are secret, the output is public to any user of the HGSC cluster. Since these compressed text files are public, they are searchable by users using zgrep.
2. When aos-scan finishes, the cron job runs the indexer which ingests the raw catalog files to generate a searchable index. This output is public to the HGSC cluster.
3. Any user of the HGSC cluster uses a new tool (probably named "search.py") to search the index in a manner inspired by the existing search.pl for the tape archive.

## searching

The main search interface is `search.py`.
It reads a text file containing search terms, one-per-line.
It generates four output files, named after the input file.
Alternatively `search-aws` accepts a single search term as a positional argument and uses standard output (stdout).

### output format

Unless otherwise noted, search results are returned as S3 URIs.

Standard columns:

- s3_uri
- size
- last_modified
- storage_class

### search.py

Use the search.py script to search the archive databases for files.
The script expects an input file consisting of a string (with possible wildcards) per line.
It returns a file with a list of paths on the archive file system that match.
You must specify which database to use or it defaults to the Production one.

#### positional arguments:

`FILE`: Input file (see `--file`)

#### Options:

`-h/--help`: Output available options

`-V/--version`: Print the version and exit

`-f/--file FILE`: Input file (`-f` is optional but `FILE` is not.)

`-m/--max-results-per-query MAX_RESULTS_PER_QUERY`: Maximum results per query. (Default is 10,000,000. No need to set this.)

`-u/--uri-only`: Suppress all output in the primary file (FILE.out.tsv) except for the S3 URIs.

#### Examples:

```shell
search.py inputlist.txt
search.py -f inputlist.txt  # The "-f" is for backwards compatibility only.
search.py inputlist.txt
```

This command will output four files:

- FILE.out.tsv: A tab-separated list of matching paths of files on the archive filesystem
- FILE.out.info: A list with extra information (match count and date archived)
- FILE.not_found.txt: A list of input lines that did not match (with errors)
- FILE.not_found.list: A clean list of input lines that did not match

Note:

- FILE is the path of input file as provided by the user.
- The output file names have changed from the old `search.pl`. See [`docs/tape-search-tools.md`](docs/tape-search-tools.md)

### search-aws

The search-aws script uses provided string to search the archive databases. You must specify which database to use or it defaults to the Production one.

#### Options:

`-h/--help`: Output available options

`-V/--version`: Print the version and exit

`-m/--max-results-per-query MAX_RESULTS_PER_QUERY`: Maximum results per query. (Default is 10,000,000. No need to set this.)

`-u/--uri-only`: Suppress all output in the primary file (FILE.out.tsv) except for the S3 URIs.

#### Example:

```shell
search-aws "search_string"
```

Matching files on the archive filesystem will be printed to standard output.

## scanning

Scanning is normally done peridically by running `aos-scan`.
Note that he CLI is still in flux.
The `aos-scan` command does no configuration of boto3;
instead this command assumes that the AWS connection is already configured through environment variables.

In production, this means a cron job that sources a secure file of secret credentials.

For developers using SSO, this means a configured profile and SSO profile and then a fresh login:

```bash
export AWS_PROFILE=scan-dev
aws sso login
```

## deployment

### production

The SysAdmin team must:

- deploy the software to a public location
- maintain the credentials (secrets) in a protected location `(drwx------)`
- maintain the cron job that
    - loads the secrets into the environment
    - runs the scanner and indexer
- maintain `/etc/profile.d/asearch.sh`

#### production configuration

```
/etc
└── profile.d
    └── asearch.sh  # adds /hgsc_software/asearch/bin to PATH
```

#### hgsc_software

```shell
# Public repo, so https does not require authentication.
REPO=https://github.com/BCM-HGSC/aws-object-search.git
mkdir -p /hgsc_software/asearch/bin
cd /hgsc_software/asearch/
git clone $REPO git
cd bin/
ln -s ../current/bin/{search-aws,search.py} .
```

#### deploying a new version

When a new public release is generated, 
Production invocation from source directory:

```shell
VERSION=1.0.0  # or whatever version is current
cd /hgsc_software/asearch/git/
git checkout v"$VERSION"
./deploy $VERSION  # Create a new directory
# ^^^ That command is equivalent to:
# ./deploy -p .. $VERSION  # Create a new directory
cd ..
# After testing...
rm -f current && ln -s "aws-object-search-$VERSION" current
```

Resulting production tree:

```
/hgsc_software/asearch
├── aws-object-search-1.0.0
│   └── bin
│       ├── aos-scan (used by cron to update s3_objects)
│       ├── search-aws
│       └── search.py
├── current -> aws-object-search-1.0.0
├── micromamba
├── git
└── s3_objects (the index of S3)
```

Currently there is no need for `AWS_OBJECT_SEARCH_CONFIG`, since we are locating s3_objects relative to the executable.

### development

First create a directory that will contain:

- the repo
- the deployed software
- the scan data

```shell
mkdir -p /path/to/aws-object-search
cd /path/to/aws-object-search
git clone git@github.com:BCM-HGSC/aws-object-search.git git
# or use the https URL if you have set up push access that way
```

The filesystem will look like this:

```
aws-object-search/
└── git/
    ├── deploy
    ├── docs/
    ├── pyproject.toml
    ├── README.md
    ├── scripts/
    ├── src/
    └── tests/
```

After running "`cd /path/to/aws-object-search/ && ./git/deploy`", the filesystem would look like this:

```
aws-object-search
├── aws-object-search-dev/  (links to git directory via "-e" option)
├── micromamba
├── git/
└── s3_objects/
```

The default suffix for the deployment directory is "-dev".
Setting that suffix to a different value (such as "-1.0.0-rc1") will result in a deployment that is frozen.
This is what happens in production.

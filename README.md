# aws-object-search

This repo is a collection of tools to facilitate searching the contents of a
collection of S3 buckets.

## Tools

- searchGlacier.py: brute-force `O(N)` metadata search of all objects within readable or specified buckets
- aos-scan: scans all buckets or those with a specified prefix to TSV files

## Technical Architecture

This is very similar to how the tape archive search system is built, but the scanning and indexing use different technologies.

### components

- updater (aos-scan)
    - scanner
    - indexer
- searcher
    - aos-search
    - search-aws
    - search.py

## execution

1. Running under a privileged account managed by the SysAdmin team, a cron job runs aos-scan  to scan buckets and generates compressed text files on local storage. Although the credentials used by aos-scan are secret, the output is public to any user of the HGSC cluster. Since these compressed text files are public, they are searchable by users using zgrep.
2. When aos-scan finishes, the cron job runs the indexer which ingests the raw catalog files to generate a searchable index. This output is public to the HGSC cluster.
3. Any user of the HGSC cluster uses a new tool (probably named "search.py") to search the index in a manner inspired by the existing search.pl for the tape archive.

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
./deploy -p .. $VERSION  # Create a new directory
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

The default suffix for the deployment directory is "-dev". Setting that suffix to a different value (such as "-1.0.0-rc1") will result in a deployment that is frozen. This is what happens in production.

# aws-object-search

This repo is a collection of tools to facilitate searching the contents of a
collection of S3 buckets.

## Tools

- searchGlacier.py: brute-force `O(N)` metadata search of all objects within readable or specified buckets
- aos-scan: scans all buckets or those with a specified prefix to TSV files

## Technical Architecture

This is very similar to how the tape archive search system is built, but the scanning and indexing use different technologies.

### components

- scanner (aos-scan)
- indexer (in progress)
- searcher (backlog)

### execution

1. Running under a privileged account managed by the SysAdmin team, a cron job runs aos-scan  to scan buckets and generates compressed text files on local storage. Although the credentials used by aos-scan are secret, the output is public to any user of the HGSC cluster. Since these compressed text files are public, they are searchable by users using zgrep.
2. When aos-scan finishes, the cron job runs the indexer which ingests the raw catalog files to generate a searchable index. This output is public to the HGSC cluster.
3. Any user of the HGSC cluster uses a new tool (probably named "search.py") to search the index in a manner inspired by the existing search.pl for the tape archive.

### deployment

When a new public release is generated, the SysAdmin must:

- deploy the software to a public location
- maintain the credentials (secrets) in a protected location (drwx------)
- maintain the cron job that runs the scanner and indexer

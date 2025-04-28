# aws-object-search

This repo is a collection of tools to facilitate searching the contents of a
collection of S3 buckets.

## Tools

- searchGlacier.py: brute-force `O(N)` metadata search of all objects within readable or specified buckets
- aos-scan: scans all buckets or those with a specified prefix to TSV files

# Tape Search Tools

Reference: https://intranet.hgsc.bcm.edu/content/search-and-verify-scripts-archived-files (requires login)

This is included to document the CLI for the old tape archive search in order to present users with a similar interface.

## search-manifest

The search-manifest script uses provided string to search the archive databases. You must specify which database to use or it defaults to the Production one.

### Options:

`-h/--help`: Output available options

`-v/--verbose`: Verbose output

`-a/--snfsa/--slow`: Use the slower archive database generated directly from snfsa (default and safest to use)

`--fast`: Use the faster archive database generated from job logs (incompatible with manual archive search)

`-o/--old/--manual`: Search manually archived files

`--production`: Search production auto archived files (default)

`--bmgl`: Search bmgl auto archived files

`--bmgl_manual`: Search bmgl manual archived files

`--hgsccl`: Search hgsccl auto archived files

`--hgsccl_manual`: Search hgsccl manual archived files

`-m/--max_results_per_query`: Maximum results per query. (default is 10000000, no need to set this)

`-c/--chksum`: When used with -a/--old/--slow this will also output the sha512 checksum file paths created during the automatic archive process

`-s/--file_sizes`: Prints out size of files. Only works with a slow search.

### Example:

```shell
search-manifest "search_string"
```

Matching files on the archive filesystem will be printed to standard output.

## search.pl

Use the search.pl script to search the archive databases for files. The script expects an input file consisting of a string (with possible wildcards) per line. It returns a file with a list of paths on the archive file system that match. You must specify which database to use or it defaults to the Production one.

### Options:

`-h/--help`: Output available options

`-f/--file`: Input file

`-a/--snfsa/--slow`: Use the slower archive database generated directly from snfsa (default and safest to use)

`--fast`: Use the faster archive database generated from job logs (incompatible with manual archive search)

`-o/--old/--manual`: Search manually archived files

`--production`: Search production auto archived files (default)

`--bmgl`: Search bmgl auto archived files

`--bmgl_manual`: Search bmgl manual archived files

`--hgsccl`: Search hgsccl auto archived files

`--hgsccl_manual`: Search hgsccl manual archived files

`-m/--max_results_per_query`: Maximum results per query. (Default is 10000000. No need to set this.)

`-c/--chksum`: When used with -a/--old/--slow this will also output the sha512 checksum file paths created during the automatic archive process

`-l/--latest`: This will select the most recent matches for each input line

`-s/--file_sizes`: Prints out size of files. Only works with a slow search.

`-v/--verbose`: Verbose output

### Example:

```shell
search.pl --bmgl --slow -f inputlist.txt
```

This command will output four files

- inputlist.txt.out: A list of matching paths of files on the archive filesystem
- inputlist.txt.out.info: A list with extra information ( match count and date archived)
- inputlist.txt.not_found: A list of input lines that did not match (with errors)
- inputlist.txt.not_found_list: A clean list of input lines that did not match

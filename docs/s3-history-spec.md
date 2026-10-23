# S3 Bucket History — Specification (Draft)

Status: draft for discussion, 2026-10-23.

This spec describes a tool that diffs periodic listings of S3 buckets
into a history of events, from which any accepted listing can be
reconstructed. It defines the events, storage format, ingestion process,
and query surface.

## Motivation

Measured on the cluster on 2026-10-22:

- `s3_objects/archive` holds 1.7 TB in 149,978 snapshot files covering
  2025-07-30 through 2026-10-21 (432 days, 448 buckets), growing about
  130 GiB per month. `/hgsc_software` has 1.1 TB free.
- Three buckets are 91% of it: `logs-bucket-136669306241` (1,283 GiB),
  `config-bucket-136669306241` (144 GiB), `hgsccl-cloudtrail-logs`
  (119 GiB).
- Day-over-day churn is 0.01–0.1% of rows. `logs-bucket` went from
  84,784,626 to 84,789,548 rows with 4,922 `new` events and nothing else.

A base snapshot plus events is therefore a few GiB instead of terabytes.

## Goals

1. The history is the **source of truth** for the past contents of every
   scanned bucket. Any accepted snapshot can be reconstructed exactly.
2. **Durability first**: no file in the history is ever modified in place;
   every file is checksummed; every reconstruction can be verified.
3. Raw snapshots can be deleted once their content is provably in the
   history.
4. Query formats are derived from the history and can be rebuilt from it
   at any time.

## Non-goals (v1)

- Rename detection. A later version could annotate a `gone` and a `new`
  in the same step whose `e_tag` and `size` match uniquely, as a
  heuristic; multipart copies may not preserve `e_tag`.
- Anything beyond the listing: `HeadObject`, tags, user metadata,
  encryption. Restore status could later come from the listing's
  `OptionalObjectAttributes=RestoreStatus`.
- S3 Event Notifications and EventBridge.
- Off-site replication. The format is designed to make it easy (immutable
  files, small ledger), but copying is a separate tool or cron job.
- Prefix-scoped series. The scanner lists whole buckets; a series is one
  bucket. The format reserves a `prefix` field for later.

## Terms

- **Snapshot**: one catalog file produced by `aos-scan` for one bucket.
- **Series**: all snapshots of one bucket, ordered by `snapshot_at`.
- **`snapshot_at`**: the listing start time in UTC, formatted
  `YYYY-MM-DDTHH:MM:SSZ`. This is the snapshot's identity within a series.
- **State**: the normalized set of rows in a snapshot, ordered by key.

## Events

The history records how a bucket changed between consecutive accepted
snapshots, diffed on `key`. Given the previous state P and the new
snapshot C, after normalization:

| Key | Event |
|---|---|
| In C, not in P | `new` |
| In P, not in C | `gone` |
| In both, any other field differs | `changed`, with `changed_fields` naming the fields |
| In both, all fields equal | None |

The first snapshot of a series emits every key as `initial`, so a real
wave of new objects is distinguishable from the start of tracking.
`initial`, `new`, and `changed` carry the object's values in the new
snapshot; `gone` carries its last known values. Prior values of a
`changed` object are not stored; earlier events hold them.

### Fields

Each row holds the fields `ListObjectsV2` returns:

- `last_modified`: when the object's current content was written.
  Lifecycle transitions do not change it.
- `size`: bytes.
- `storage_class`: changes on lifecycle transition without touching
  `last_modified`.
- `e_tag`: MD5 for single-part uploads without KMS, otherwise opaque. Not
  a reliable content hash across copies.
- `checksum_algorithm`, `checksum_type`: which additional checksums
  exist, and whether they are `FULL_OBJECT` or `COMPOSITE`. The checksum
  values are not in the listing.

### Interpreting `changed_fields`

These are hints for readers, not separate event types:

| Differing fields | Likely cause |
|---|---|
| `last_modified` + `e_tag` (often `size`) | Content overwritten |
| `last_modified` only | Same bytes rewritten (copy onto itself for a metadata or encryption change, or an identical re-upload) |
| `storage_class` only | Lifecycle transition |
| `storage_class` + `last_modified` | Manual class change by copy onto itself |
| `checksum_*` + `last_modified` | Rewritten with different checksum settings |

### Core invariant

For every accepted snapshot S of a series:

```
reconstruct(history up to S.snapshot_at) == normalize(S)
```

Reconstruction applies events in time order, as upserts and deletes (see
"Reconstruction"). `state_sha256` makes the invariant checkable without
keeping S. It is the primary test.

### Known limits

The history records net differences between snapshots, not every change:

- Several overwrites of one key between snapshots appear as one
  `changed`.
- A delete followed by a re-upload under the same key appears as
  `changed`.
- An object created and deleted within one interval leaves no trace.
- A rename appears as `gone` plus `new`. S3 has no rename, only copy and
  delete.
- A listing is not a point-in-time image: objects can change while it
  paginates. A `gone` is known only to have happened between two
  snapshots; listings carry no deletion time.
- Changes to tags, user metadata, ACLs, encryption, Object Lock, and
  Glacier restore status are invisible unless they rewrite the object,
  which shows as `last_modified` changing.
- In versioned buckets, listings show only current versions; delete
  markers and noncurrent versions are not recorded.

## Prerequisite: scanner changes

The scanner currently writes each catalog directly to its final name while
paginating, so a failed listing leaves a truncated file that looks like a
normal one, and nothing records when the listing finished. Before any new
snapshot is ingested:

1. Write the catalog to `NAME.tsv.gz.partial`, then `fsync` and rename it to
   `NAME.tsv.gz` only after the final page.
2. Then write a sidecar `NAME.meta.json` (same temp-and-rename discipline)
   containing:

   ```json
   {
     "bucket": "hgsc-smaht",
     "prefix": "",
     "listing_started_at": "2026-10-21T08:15:02Z",
     "listing_completed_at": "2026-10-21T08:21:40Z",
     "rows": 580105,
     "complete": true,
     "scanner_version": "1.1.0"
   }
   ```

   Times are UTC. The catalog file name keeps its current local-time
   format so that existing tools (`current_bucket_scans`, `zgrep` users)
   are unaffected.
3. A failure while listing one bucket logs an error, removes the
   `.partial` file, and continues with the next bucket instead of ending
   the run.

The sidecar does not match the catalog glob patterns, so
`S3ObjectCatalog` ignores it. `archive_old_scans` moves it with its
catalog.

## Storage layout

```
s3_objects/history/
├── FORMAT                                      "aos-history 1"
├── .lock
└── <bucket>/
    ├── ledger.tsv
    ├── <bucket>.base.<snapshot_at>.tsv.gz
    ├── <bucket>.<YYYY-MM>.events.tsv.gz        sealed monthly segment
    └── staging/
        └── <bucket>.<snapshot_at>.events.tsv.gz
```

`<snapshot_at>` in file names uses the compact form `YYYYMMDDTHHMMSSZ`.
Bucket names are safe in file names (lowercase letters, digits, `.`, `-`).
Every data file carries its bucket in its name, so a file copied out of
its directory still identifies itself.

### Data files

Base, staging, and segment files share one format: gzip-compressed TSV
with a header row, written with the same `csv` dialect as catalog files
(tab delimiter, `csv` quoting for fields containing tabs, quotes, or
newlines, UTF-8).

| Column | Notes |
|---|---|
| `event` | `initial`, `new`, `changed`, or `gone` |
| `snapshot_at` | UTC, `YYYY-MM-DDTHH:MM:SSZ` |
| `prev_snapshot_at` | Empty for `initial` |
| `key` | |
| `last_modified`, `size`, `storage_class`, `e_tag`, `checksum_algorithm`, `checksum_type` | Normalized; for `gone`, the last known values |
| `changed_fields` | `:`-joined field names in the column order above; `changed` only |

Row order:

- **Base**: only `initial` events, ordered by key.
- **Staging** (one snapshot): ordered by key.
- **Segment** (one month): ordered by key, then `snapshot_at`. All of one
  key's events for the month are adjacent and in time order.

Key order everywhere is binary order of the UTF-8 bytes. This is the order
`ListObjectsV2` returns and the order the existing catalogs are in
(checked on four buckets, including `logs-bucket`).

Ordering segments by key rather than by time means that a replay is a
single merge across the base and the segments, one stream per file, in
constant memory (see "Reconstruction").

Gzip output is written reproducibly: header `mtime` 0, no embedded file
name, compression level 6. With the same zlib build, re-running a step
produces identical files. Nothing depends on that, because another zlib
build may compress differently: a file is checked against the
`file_sha256` in its ledger record, never against regenerated bytes.

### Normalization

Applied to every row read from a catalog before diffing or hashing.
Without it, formatting differences would show as spurious `changed`
events:

- `last_modified`: parse, convert to UTC, format
  `YYYY-MM-DDTHH:MM:SS+00:00`, keeping fractional seconds only when
  non-zero.
- `e_tag`: strip one pair of surrounding double quotes if present (the
  scanner already does this).
- `checksum_algorithm`: split on `:`, sort, rejoin. Absent and empty are
  both `""`.
- Other absent optional fields: `""`.

### Ledger

`ledger.tsv` is an uncompressed, append-only TSV with a header row. One
line per record. It is the index of the series: a data file belongs to
the history only when a ledger record names it, and anything else in the
directory is garbage left by an interrupted run.

| Column | Notes |
|---|---|
| `record` | `base`, `snapshot`, `rejected`, or `seal` |
| `snapshot_at` | Empty for `seal` |
| `prev_snapshot_at` | `snapshot` only |
| `listing_completed_at` | From the sidecar; for backfilled snapshots, the catalog's mtime |
| `period` | `YYYY-MM`; `seal` only |
| `source` | Catalog file name the record was derived from |
| `rows` | Rows in the snapshot (`base`, `snapshot`) |
| `n_new`, `n_changed`, `n_gone` | `snapshot` only |
| `file` | Data file written (empty for a `snapshot` with zero events) |
| `file_sha256` | SHA-256 of `file` |
| `state_sha256` | State digest of the snapshot (`base`, `snapshot`) |
| `note` | Reason for `rejected`; free text otherwise |
| `recorded_at` | UTC time the record was written |
| `tool_version` | |
| `crc32` | CRC-32 of the line up to and including the preceding tab, as 8 hex digits |

Records:

- **`base`**: the state at `snapshot_at` is stored in full in `file`. The
  first accepted snapshot of a series is always a `base`.
- **`snapshot`**: the snapshot was accepted and diffed against
  `prev_snapshot_at`. Its events are in `file` until the month is sealed,
  then in that month's segment.
- **`rejected`**: the snapshot failed validation and was not diffed. The
  next accepted snapshot diffs against the last accepted one, so its
  `prev_snapshot_at` spans the gap.
- **`seal`**: all `snapshot` events with `snapshot_at` in `period` are now
  in the segment `file`. Staging files for that period may be deleted.

A `snapshot` record with an empty `file` is how the history says "checked,
no change"; without it a quiet day and a missed day would look the same.

Readers must:

- verify `crc32` on every line,
- ignore a final line that lacks a trailing newline or fails its CRC (a
  torn append), and report it,
- treat a bad line anywhere else as corruption and stop.

### State digest

`state_sha256` is SHA-256 over the state in key order, each row encoded
as its seven normalized fields (`key` first, then the six metadata fields
in the column order above), each field as its UTF-8 byte length in
decimal, `:`, then the bytes. Length prefixing makes the encoding
unambiguous for keys containing any character.

The digest lets any reconstruction be checked against the ledger without
keeping the original snapshot.

## Write discipline

Every file in `history/` is created by:

1. writing to `NAME.tmp` in the same directory,
2. `fsync` of the file,
3. `rename` to `NAME`,
4. `fsync` of the directory.

Ledger records are appended with a single `write` of the whole line
followed by `fsync`. A data file is always durable before the ledger
record that names it.

Base and segment files are made read-only (`0444`) once written. Nothing
in `history/` is ever rewritten, with two exceptions:

- the ledger, which only grows,
- staging files of a sealed period, which are deleted after the `seal`
  record is durable.

All `history/` writes hold an exclusive `flock` on `history/.lock`.

## Ingestion

`aos-history ingest` runs after the scan and before indexing and
archiving. For each bucket with a catalog newer than the ledger's last
record, oldest first:

1. **Skip** if a record for this `snapshot_at` already exists
   (idempotent).
2. **Validate** the snapshot. Reject it, with a `rejected` record, if:
   - its sidecar is missing or `complete` is not `true` (new snapshots
     only; see "Backfill" for archived ones),
   - the gzip stream is not intact through end-of-file,
   - the header is not the catalog header,
   - the sidecar's `bucket` is not the series' bucket, taken from the
     catalog file name, or its `prefix` is not empty (see "Non-goals"),
   - keys are not strictly increasing (catches duplicates and ordering),
   - the row count differs from the sidecar's,
   - `snapshot_at` is not later than the last accepted snapshot.
3. **Hold** rather than reject if the row count fell by more than
   `--max-drop` (default 10%) from the previous accepted snapshot. A held
   snapshot is reported and not recorded; it can be accepted with
   `--accept SNAPSHOT_AT` after review. A partial listing that looks
   complete would otherwise record a mass of false `gone` events.
4. **Diff** against the previous accepted state with a streaming merge
   join, producing events in key order and computing `state_sha256` of
   both inputs as they stream.
   - The previous state comes from the previous raw catalog if it is still
     present. Its digest must equal the ledger's; on a mismatch, stop and
     report, because the raw file no longer matches the history.
   - If the previous raw catalog is gone, the previous state is
     reconstructed from the history.
5. **Write** the staging file (if there are any events), then append the
   `snapshot` record.
6. **Seal** any month that is earlier than this snapshot's month and has
   not yet been sealed (see below).

The first snapshot of a bucket writes a base file and a `base` record
instead of steps 4–5.

A new base is also written for the first accepted snapshot of each
calendar year (`--rebase-yearly`, on by default), in addition to that
snapshot's normal diff and `snapshot` record, so no events are lost at the
boundary. This bounds replay to one base plus at most twelve segments. The
yearly base for `logs-bucket` is about 3.3 GB.

`aos-history rebase BUCKET` ingests the bucket's next candidate as a
base. It is for a series whose earlier history is damaged, so it does
not read that history: the candidate is validated (step 2) but neither
held nor diffed, and gets only a `base` record, written as for a first
snapshot. The new base starts a chain that does not depend on anything
before it. The gap since the previous accepted snapshot has no events.

### Sealing a month

1. K-way merge the period's staging files into one segment, ordered by
   key then `snapshot_at`.
2. Re-read the segment and check that it holds exactly the period's
   events: per-snapshot counts must match the `snapshot` records.
3. Verify the replay (below) at the period's last snapshot against its
   `state_sha256`.
4. Append the `seal` record, then delete the period's staging files.

If the process dies between steps, rerunning writes the segment again,
replacing any left from the failed run (it is garbage until a `seal`
record names it), and continues.

## Reconstruction

The state at accepted snapshot S is a key-ordered merge of:

- the latest `base` B at or before S,
- every segment and staging file that may hold events in (B, S],

applying, for each key, its events with B < `snapshot_at` ≤ S in time
order: `new`/`changed` set the row, `gone` removes it. Each input is
already key-ordered, so the merge uses one open stream per file and
constant memory regardless of bucket size. Files are taken in time order,
so a key's events gathered across them are already in time order.

The result is written in catalog format, so a reconstructed snapshot is
a drop-in replacement for an archived catalog file.

## Verification

`aos-history verify [BUCKET...]` checks, per series:

- every ledger line's CRC, and that records are in `snapshot_at` order,
- every referenced file exists and matches `file_sha256`,
- every segment holds exactly the events its records count,
- the replay at the last snapshot of each sealed period, and at the latest
  snapshot, matches `state_sha256`,
- no unreferenced files are present (reported; removed with `--clean`).

The daily diff already checks each snapshot pair as it goes; `verify` is
the end-to-end check run at sealing and before pruning.

## Pruning raw snapshots

`aos-history prune --keep-days N` (default 14) deletes archived catalogs
(and their sidecars) older than N days, and only when:

- the ledger has a `base` or `snapshot` record for that catalog, and
- `verify` has passed for that bucket since the record was written, or
  the catalog's `snapshot_at` is covered by a sealed, verified period.

Rejected catalogs are never pruned automatically. Archiving moves them
to `archive/rejected/` for review. As with other catalogs, it moves one
only once a later catalog of the bucket exists, so a rejected catalog
that is still the bucket's latest stays at the top level and is indexed,
as today.

Keeping recent raw catalogs means the daily diff normally reads two raw
files and never needs a reconstruction.

## Backfill

`aos-history backfill ARCHIVE_DIR` ingests the existing archive, oldest
first, through the same code path, with these differences for catalogs
that have no sidecar:

- `snapshot_at` comes from the file name, read as `America/Chicago` local
  time and converted to UTC. The nightly runs start around 03:15, after
  the 02:00 daylight-saving switch, so the conversion is unambiguous; a
  name that is ambiguous or nonexistent is rejected.
- `listing_completed_at` is the file's mtime. (Archiving renames the file
  and keeps its mtime. On 2026-10-21, `logs-bucket` listed from 06:51 to
  14:54 local.)
- Completeness cannot be proven, so the remaining checks stand in:
  gzip integrity, header, strict key order, and the row-count drop hold.

Buckets are independent and can be backfilled in parallel. Backfill does
not prune; pruning the archive is a separate, deliberate step after
`verify` passes for every bucket.

Throughput still needs measuring. A diff of two `logs-bucket` snapshots
(85 M rows each) is the dominant cost and runs 432 times.

## Query support

Everything in this section is derived from `history/` and can be deleted
and rebuilt.

**v1, built into `aos-history`:**

- `aos-history as-of BUCKET DATE [-o FILE]`: reconstruct the state at the
  last accepted snapshot on or before DATE, in catalog format. Answers
  "what was in this bucket then?" and supports `zgrep` workflows.
- `aos-history log BUCKET [KEY_OR_PREFIX]`: print events for a key or
  prefix in time order, across bases and segments. Answers "when did this
  change or disappear?". Uses the key order of every file to seek past
  earlier keys rather than scan everything.
- `aos-history status [BUCKET...]`: per-series summary from the ledger:
  first and last snapshot, gaps, rejected and held snapshots, and sizes.

**Later:**

- An "ever seen" Tantivy index: one document per (bucket, key) with
  first-seen and last-seen times and last known metadata, behind
  `search-aws --history`. This answers the most common user question,
  "did this file ever exist in S3, and where did it go?", with the search
  tool users already know.
- Parquet export of events for ad hoc analysis with DuckDB.

## Package layout

- `src/aws_object_search/history/`: format, ledger, diff, merge, digest,
  and the `aos-history` entry point.
- Tests cover the core invariant on synthetic series, including keys with
  tabs and newlines, rejected and held snapshots, rebasing, sealing, torn
  ledger lines, and an interrupted run at every write step.

## Open questions

- Should `logs-bucket-136669306241`, `config-bucket-136669306241`, and the
  CloudTrail buckets be scanned at all? If no one searches them, excluding
  them also shrinks the 19 GB search index.
- Where does the off-site copy go: an S3 bucket with versioning and
  Object Lock, tape, or both? Is `/hgsc_software` (`sug-hnas01:/test_software/8`)
  itself backed up?
- Does the 1.7 TB archive get a cold copy before it is pruned, or is a
  verified history enough?
- Default `--keep-days` and `--max-drop` values.
- Should `ingest` be a step of `aos-scan` (with `--no-history`) or its own
  cron entry?

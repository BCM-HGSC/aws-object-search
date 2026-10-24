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
  files, small ledger), but copying the history off the filer is left to
  the SysAdmin team. `/hgsc_software` has HNAS snapshots, but those are on
  the same filer and do not count as an off-site copy.
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

1. Write the catalog to `NAME.tsv.gz.partial` and `fsync` it after the
   final page.
2. Write a sidecar `NAME.meta.json` (same temp-and-rename discipline),
   then rename the catalog to `NAME.tsv.gz`. A catalog under its final
   name therefore always has its sidecar. The sidecar contains:

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
   the run. A credential error (an expired SSO token, missing
   credentials) still ends the run, since every remaining bucket would
   fail the same way. If any bucket failed, `aos-scan` exits non-zero
   once the run is done, so cron reports it.
4. A crash can still leave a `.partial` file, or a sidecar whose catalog
   was never renamed. Nothing reads either: ingestion, archiving,
   indexing, and pruning use only final catalog names, and the next scan
   puts a new time in its names. They are not removed automatically,
   because a `.partial` file may belong to a scan that is still running.
   `aos-scan` and `aos-history status` report them, and an admin removes
   them with `aos-history clean` once no scan is running.

The sidecar does not match the catalog glob patterns, so
`S3ObjectCatalog` ignores it. `archive_old_scans` moves it with its
catalog, holding the bucket's history lock (see "Write discipline";
taken without waiting, and a locked bucket is archived on the next
run), catalog first, then sidecar. A crash between the two renames
leaves the sidecar at the top level and its catalog in `archive/`.
Archiving finds such a sidecar on its next run and finishes the move.
`clean` treats a sidecar as orphaned only when its catalog is neither at
the top level nor in `archive/`.

## Storage layout

```
s3_objects/history/
├── FORMAT                                      "aos-history 1"
└── <bucket>/
    ├── .lock
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
newlines, UTF-8), except that lines end in `\n`. Catalog files end lines
in `\r\n` (the `csv` module default); history files do not, so `zgrep`
and exact matching work without stripping `\r`.

| Column | Notes |
|---|---|
| `event` | `initial`, `present`, `new`, `changed`, or `gone` |
| `snapshot_at` | UTC, `YYYY-MM-DDTHH:MM:SSZ` |
| `prev_snapshot_at` | Empty for `initial` and `present` |
| `key` | |
| `last_modified`, `size`, `storage_class`, `e_tag`, `checksum_algorithm`, `checksum_type` | Normalized; for `gone`, the last known values |
| `changed_fields` | `:`-joined field names in the column order above; `changed` only |

Row order:

- **Base**: ordered by key. The first base of a series holds only
  `initial` rows. Later bases (yearly or forced rebases) hold only
  `present` rows. A `present` row is not an event: it restates a key that
  the history already tracks, so `initial` keeps its meaning of "start of
  tracking". Replay treats both as setting the row.
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

- Line endings: read catalogs with the `csv` module and `newline=""`,
  which treats `\r\n` as the line end. Splitting raw lines would leave
  `\r` on the last field, `key`.
- `last_modified`: parse, convert to UTC, truncate to whole seconds, and
  format `YYYY-MM-DDTHH:MM:SS+00:00`. A change of less than a second is
  therefore not a `changed` event. Users need the date, and perhaps the
  hour, of a change; the exact format matters only because
  `state_sha256` hashes it.
- `e_tag`: strip one pair of surrounding double quotes if present (the
  scanner already does this).
- `checksum_algorithm`: split on `:`, sort, rejoin. Absent and empty are
  both `""`.
- Other absent optional fields: `""`.

### Ledger

`ledger.tsv` is an uncompressed, append-only TSV with a header row. One
line per record. Fields are never quoted: a tab, carriage return, or
newline in `note` is replaced with a space, and no other field can
contain one. It is the index of the series: a data file belongs to
the history only when a ledger record names it, and anything else in the
directory is garbage left by an interrupted run.

| Column | Notes |
|---|---|
| `record` | `base`, `snapshot`, `held`, `rejected`, or `seal` |
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
| `note` | Reason for `held` or `rejected`; free text otherwise |
| `recorded_at` | UTC time the record was written |
| `tool_version` | |
| `crc32` | CRC-32 of the line up to and including the preceding tab, as 8 hex digits |

Records:

- **`base`**: the state at `snapshot_at` is stored in full in `file`. The
  first accepted snapshot of a series is always a `base`.
- **`snapshot`**: the snapshot was accepted and diffed against
  `prev_snapshot_at`. Its events are in `file` until a `seal` record
  covers its month, then in that seal's segment. Once sealed, `file` and
  `file_sha256` record what was staged; they no longer name a file that
  must exist.
- **`held`**: the snapshot failed a hold test (see "Ingestion") and is
  waiting for an operator. Written once, when the snapshot is first
  held. A later `snapshot`, `base`, or `rejected` record for the same
  `snapshot_at` resolves it.
- **`rejected`**: the snapshot failed validation and was not diffed. The
  next accepted snapshot diffs against the last accepted one, so its
  `prev_snapshot_at` spans the gap.
- **`seal`**: all `snapshot` events with `snapshot_at` in `period` are now
  in the segment `file`. Staging files for that period may be deleted.

A `snapshot` record with an empty `file` is how the history says "checked,
no change"; without it a quiet day and a missed day would look the same.

Readers must:

- verify `crc32` on every line after the header,
- ignore a final line that lacks a trailing newline (a torn append), and
  report it,
- treat any other bad line as corruption and stop. This includes a final
  line that ends in a newline but fails its CRC: it may be a complete,
  acknowledged record that was damaged later, so it is not discarded
  automatically.

Before appending, the writer truncates a torn final line, logging its
bytes, so that the new record does not run on from the fragment. A line
without its newline was never acknowledged, so nothing it names is part
of the history. Truncating it is the one change ever made to existing ledger bytes.

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

- the ledger, which only grows, apart from removing a torn final line,
- staging files of a sealed period, which are deleted after the `seal`
  record is durable.

All writes to a series hold an exclusive `flock` on `<bucket>/.lock`, so
different buckets can be written in parallel. Ingestion in `aos-scan`
takes the lock without waiting and skips a locked bucket, reporting it;
backfill holds a bucket's lock for its whole run. `verify --clean` and
`clean` delete files, so they also take the exclusive lock.

Commands that read data files (`as-of`, `log`, and `verify` without
`--clean`) hold a shared lock on `<bucket>/.lock`, waiting for it, so
that a seal cannot delete staging files while they read. Nightly
ingestion skips a bucket that a reader holds, as it does one that
backfill holds. `status` reads only the ledger, which is append-only,
and takes no lock.

Code that already holds a bucket's lock, such as the replay check in
sealing or the `verify` that `prune --backfilled` runs, works under that
lock and never opens `.lock` again. Besides deadlocking on a local
filesystem, a second lock is unsafe on NFS, where `/hgsc_software`
lives: Linux emulates `flock` there with POSIX record locks, which
belong to the process, so a second request converts the lock instead of
waiting, and closing the second descriptor releases it.

## Configuration

An optional TOML file sets per-bucket policy. `aos-scan` reads it from
`--config PATH`, or by default from `aws-object-search.toml` beside the
output root (in production,
`/hgsc_software/aws-object-search/aws-object-search.toml`, shared by all
releases; in development, the repo root, where `.gitignore` must list
it). Without the file, every listed bucket is scanned, recorded,
and indexed, as today.

```toml
[buckets]
# Glob patterns matched against bucket names.
# Scanned and recorded in the history, but left out of the search index.
history_only = [
    "logs-bucket-136669306241",
    "config-bucket-136669306241",
    "*cloudtrail-logs*",
]
# Not scanned at all. Their history ends with their last snapshot.
exclude = []

[history]
keep_days = 14            # --keep-days
max_drop_fraction = 0.10  # --max-drop-fraction
max_drop_rows = 1000      # --max-drop-rows
```

Command-line options override the file; the values shown are the
defaults when neither sets them.

A bucket matching both lists is excluded. History-only catalogs still
sit in `s3_objects/` like any other, so `zgrep` users can read them.

## Ingestion

Ingestion is a step of `aos-scan`, on by default and turned off with
`--no-history`. It is also available on its own as `aos-history ingest`.
One `aos-scan` run does, in order:

1. scan (unless `--no-scan`),
2. ingest every catalog in the output root that the ledgers have not
   recorded (unless `--no-history`),
3. index and archive (unless `--no-index`),
4. prune (only when step 3 ran, unless `--no-prune`).

Production scans two billing accounts into one output root with two runs:
the first with `--no-index`, the second indexing both accounts' catalogs.
Ingesting in both runs records each account's snapshots right after its
scan. Because archiving happens only in the indexing run, the previous
catalog of a bucket may be at the top level of the output root or under
`archive/`; ingestion looks for it in both, by the `source` name in the
ledger.

The candidates for a bucket are its catalogs that lack a record they
need (see step 1), at the top level or under `archive/` (not
`archive/rejected/`), taken oldest first. Usually these are the
unrecorded ones; a catalog whose `snapshot` record was written but whose
yearly base was not is also a candidate.
"Last accepted snapshot" below means the last `base` or `snapshot`
record; `held`, `rejected`, and `seal` records do not count.

Catalogs without a sidecar predate the scanner changes and are ingested
only by backfill. A bucket with an unrecorded catalog that has no
sidecar is reported as awaiting backfill and skipped, so a nightly run
cannot write a base ahead of the archive or reject the archive for its
missing sidecars. Archived catalogs that have sidecars are ordinary
candidates, so a new bucket whose first catalog was archived before it
was ingested (after a `--no-history` run, for example) is still started.

For each candidate:

1. **Complete or skip.** If the ledger already has every record this
   `snapshot_at` needs (`snapshot`, plus `base` when a rebase is due), skip
   it. If it has the `snapshot` record but lacks a due yearly `base`,
   write only the base, without repeating steps 2–7: the snapshot passed
   them when its record was written. A rerun after a crash therefore
   finishes the work rather than skipping it.
2. **Validate** the snapshot. Reject it, with a `rejected` record, if:
   - its sidecar's `complete` is not `true` (catalogs without a sidecar
     go through "Backfill" instead),
   - the gzip stream is not intact through end-of-file,
   - the header is not the catalog header,
   - the sidecar's `bucket` is not the series' bucket, taken from the
     catalog file name, or its `prefix` is not empty (see "Non-goals"),
   - keys are not strictly increasing (catches duplicates and ordering),
   - the row count differs from the sidecar's,
   - `snapshot_at` is not later than the last accepted snapshot.
3. **Diff** against the previous accepted state with a streaming merge
   join, writing events in key order to the staging file's `NAME.tmp`
   and computing `state_sha256` of both inputs as they stream. The diff
   also counts the events of each kind and the `gone` keys after the new
   snapshot's last key, for step 4.
   - The previous state comes from the previous raw catalog if it is still
     present. Its digest must equal the ledger's; on a mismatch, stop and
     report, because the raw file no longer matches the history.
   - If the previous raw catalog is gone, the previous state is
     reconstructed from the history. Its digest must also equal the
     ledger's; on a mismatch, stop and report, because the history no
     longer reproduces the recorded state.
4. **Hold** rather than accept if, compared with the previous accepted
   snapshot, either:
   - the row count fell by more than `max_drop_fraction` (default 10%)
     **and** by more than `max_drop_rows` (default 1,000), or
   - the last key is earlier than the previous snapshot's last key, and
     more than `max_drop_rows` keys after it are gone. A truncated
     listing loses the end of the key order, so this catches truncation
     too small for the row-count test, such as several million rows of
     `logs-bucket`.

   The row floor keeps small buckets that empty out from being held. A
   partial listing that looks complete would otherwise record a mass of
   false `gone` events.

   A held snapshot's `NAME.tmp` is deleted, and the snapshot gets a
   `held` record, so the hold is visible from the ledger. A hold blocks
   the series: the held snapshot and every later catalog of that bucket
   stay without a `base` or `snapshot` record, and so unpruned, until an
   operator runs `aos-history ingest --accept BUCKET SNAPSHOT_AT` or
   `--reject BUCKET SNAPSHOT_AT`. Ingestion checks the ledger first and
   skips a series with an unresolved `held` record without reading its
   catalogs, so the held snapshot is not diffed again each night, and a
   later change to the thresholds cannot accept it without the operator.
   Each run reports the blocked bucket. Other buckets are unaffected.
5. **Write** the staging file by finishing its write discipline (see
   "Write discipline"), or delete `NAME.tmp` if there are no events.
6. **Check the step.** Re-read the staging file from disk and apply it to
   the previous state, streaming that state again. The digest of the
   result must equal the new snapshot's `state_sha256`. On a mismatch,
   stop and report; nothing is recorded. This check, not the diff, proves
   that the previous state plus the stored events reproduces the new
   snapshot. Because every step is checked, each accepted snapshot is
   reproducible by induction from its base. For `logs-bucket` it costs a
   second read of a roughly 3 GiB catalog per day.
7. **Append** the `snapshot` record.
8. **Seal** any month that is earlier than this snapshot's month and has
   not yet been sealed (see below), and delete any staging files left
   over from months already sealed.

"Stop and report" in steps 3 and 6 stops only that bucket, for this
run, with nothing recorded. Other buckets are ingested, indexing and
pruning still run, and `aos-scan` exits non-zero once the run is done.
The next run meets the same mismatch, so the bucket stays stopped until
an operator investigates.

The first snapshot of a bucket writes a base file and a `base` record
instead of steps 3–7. The base file is re-read from disk and its digest
checked against the snapshot before the record is appended.

A new base, with `present` rows, is also written for the first accepted
snapshot of each calendar year in UTC (`--rebase-yearly`, on by default). It
comes after that snapshot's normal diff and `snapshot` record, so no
events are lost at the boundary. This bounds replay to one base plus at
most twelve segments. The yearly base for `logs-bucket` is about 3.3 GB.

`aos-history rebase BUCKET` ingests the bucket's next candidate as a
base. It is for a series whose earlier history is damaged, so it does
not read that history: the candidate is validated (step 2) but neither
held nor diffed, and gets only a `base` record. The base has `present`
rows and is re-read and checked like a first base. It starts a chain
that does not depend on anything before it. The gap since the previous
accepted snapshot has no events.

### Sealing a month

A month's staging files are the `file` values of its `snapshot`
records.

Ingestion seals a month when a snapshot from a later month arrives. A
series that has ended, because its bucket was excluded or deleted, gets
no such snapshot, so `status` reports a series as ended when its last
accepted snapshot is in a month (UTC) that ended more than two days ago
and is not sealed, and `aos-history seal BUCKET` seals the ended
months of that series. The two days cover the scans and ingestion that
finish after midnight UTC on the 1st. A series that is blocked, by an
unresolved `held` record or by catalogs that have no ledger record (an
unfinished backfill), is reported as blocked, not ended, and `seal`
refuses to run on it. Sealing it would strand the blocked catalogs,
because once a month is sealed, a snapshot from that month is rejected.

1. K-way merge the period's staging files into one segment, ordered by
   key then `snapshot_at`.
2. Re-read the segment and check that it holds exactly the period's
   events: per-snapshot counts must match the `snapshot` records.
3. Verify the replay (below) at the period's last snapshot against its
   `state_sha256`, reading the new segment in place of the period's
   staging files, as if the seal were already recorded. Otherwise the
   replay would read only the staging files, and the segment that is
   about to replace them would go unchecked.
4. Append the `seal` record, then delete the period's staging files.

If the process dies between steps, rerunning writes the segment again,
replacing any left from the failed run (it is garbage until a `seal`
record names it), and continues. If it dies after the `seal` record but
before the staging files are deleted, the seal is not rerun. The
leftover files are still named by `snapshot` records but are no longer
part of the history; the next ingestion of the series deletes them, and
`verify` reports them.

## Reconstruction

The state at accepted snapshot S is a key-ordered merge of:

- the latest `base` B at or before S,
- the segments named by `seal` records and the staging files of
  unsealed months named by `snapshot` records, that hold events in
  (B, S]. Staging files of a sealed month that have not been deleted yet
  are not read,

applying, for each key, its events with B < `snapshot_at` ≤ S in time
order: `new`/`changed` set the row, `gone` removes it. Each input is
already key-ordered, so the merge uses one open stream per file and
constant memory regardless of bucket size. Files are taken in time order,
so a key's events gathered across them are already in time order.

The result is written in catalog format, so a reconstructed snapshot is
a drop-in replacement for an archived catalog file.

## Verification

`aos-history verify [BUCKET...]` checks, per series:

- every ledger line's CRC, and that `base` and `snapshot` records are in
  `snapshot_at` order (`held`, `rejected`, and `seal` records are
  exempt),
- every file that must exist matches `file_sha256`: bases, segments
  named by `seal` records, and staging files of unsealed months,
- every segment holds exactly the events its records count,
- the replay at the last snapshot of each sealed period, and at the latest
  snapshot, matches `state_sha256`,
- no unreferenced files, and no staging files of sealed periods, are
  present (reported; removed with `--clean`, under the exclusive lock).

The step check in ingestion already proves each snapshot from the one
before it; `verify` is the end-to-end check run at sealing and before
pruning the backfilled archive.

## Pruning raw snapshots

Pruning runs as the last step of an indexing `aos-scan` run, and on its
own as `aos-history prune`. It deletes archived catalogs (and their
sidecars) older than `keep_days` (default 14; see "Configuration"), and
only when:

- the catalog has a sidecar. A catalog without one belongs to the
  backfilled archive, which only `aos-history prune --backfilled` deletes,
  after `verify` passes for its bucket (see "Backfill"),
- the ledger has a `base` or `snapshot` record for that catalog, which
  ingestion appends only after the step check (or, for a base, the
  re-read check) has passed, and
- that record's data file, if any, still matches its `file_sha256`; for
  a `snapshot` in a sealed month, the segment must match the checksum in
  its `seal` record instead.

Pruning holds each bucket's lock while it deletes that bucket's
catalogs, taking it without waiting and skipping a locked bucket, so it
never deletes a catalog that ingestion or backfill is about to read.

The full-replay check in `verify` runs at each monthly seal. It is not a
precondition for each daily prune, because a full replay of `logs-bucket`
reads its 3.3 GB base every time; the step checks already chain each
snapshot to its base.

Rejected catalogs are never pruned automatically. Archiving moves them
to `archive/rejected/` for review. As with other catalogs, it moves one
only once a later catalog of the bucket exists, so a rejected catalog
that is still the bucket's latest stays at the top level and is indexed,
as today.

Keeping recent raw catalogs means the daily diff and step check normally
read two raw files and never need a reconstruction.

## Backfill

`aos-history backfill [BUCKET...]` ingests the catalogs without sidecars,
oldest first, through the same code path. It reads them from the top
level of the output root as well as `archive/` (not `archive/rejected/`):
the last catalog of a bucket that was deleted or excluded before the
scanner changes is never archived, and without it the bucket would wait
for backfill forever. Catalogs without a sidecar differ in these ways:

- `snapshot_at` comes from the file name, read as `America/Chicago` local
  time and converted to UTC. The nightly runs start around 03:15, after
  the 02:00 daylight-saving switch, so the conversion is unambiguous; a
  name that is ambiguous or nonexistent is rejected.
- `listing_completed_at` is the file's mtime. (Archiving renames the file
  and keeps its mtime. On 2026-10-21, `logs-bucket` listed from 06:51 to
  14:54 local.)
- Completeness cannot be proven, so the remaining checks stand in:
  header, strict key order, and both hold tests. Gzip integrity proves
  little here, because the old scanner closed the gzip stream cleanly
  even when the listing failed.

A hold during backfill stops that bucket's backfill. The operator
resolves it with `aos-history ingest --accept BUCKET SNAPSHOT_AT` or
`--reject BUCKET SNAPSHOT_AT`, which act on the held catalog even though
it has no sidecar, and then reruns `backfill` for the bucket, which
continues from the next unrecorded catalog.

Buckets are independent and can be backfilled in parallel, one process
per bucket, each holding that bucket's lock. Nightly ingestion can stay
on throughout: it skips a bucket that has unrecorded catalogs without
sidecars (see "Ingestion"), and a bucket while backfill holds its lock.
Backfill does not prune, and neither does nightly pruning touch the
catalogs it recorded, since they have no sidecars. Once backfill has
finished, `aos-history prune --backfilled` takes each bucket's
exclusive lock, runs `verify` on it under that lock, and prunes the
archived catalogs of those that pass; no separate copy of
the 1.7 TB archive is kept.

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
  change or disappear?". Key order lets it stop reading each file once
  past the key or prefix, but gzip cannot seek, so it decompresses each
  file up to that point: for a late key in `logs-bucket`, most of the
  3.3 GB base. Shows `present` rows only
  when the first base it reads is a later base, to give the starting
  values.
- `aos-history status [BUCKET...]`: per-series summary from the ledger:
  first and last snapshot, gaps, rejected and held snapshots, blocked
  and ended series (see "Sealing a month"), and sizes.
  It also reports stale `.partial` files and orphaned sidecars in the
  output root.
- `aos-history clean`: remove stale `.partial` files and orphaned
  sidecars (those whose catalog is neither at the top level nor in
  `archive/`). Run by an admin once no scan is running.

**Later:**

- An "ever seen" Tantivy index: one document per (bucket, key) with
  first-seen and last-seen times and last known metadata, behind
  `search-aws --history`. This answers the most common user question,
  "did this file ever exist in S3, and where did it go?", with the search
  tool users already know.
- Parquet export of events for ad hoc analysis with DuckDB.
- Seekable history files (BGZF blocks with a sparse key index), if `log`
  is too slow on the large buckets.

## Package layout

- `src/aws_object_search/history/`: format, ledger, diff, merge, digest,
  and the `aos-history` entry point.
- Tests cover the core invariant on synthetic series, including keys with
  tabs and newlines, rejected and held snapshots (including a blocked
  series), truncated listings, a failing step check, rebasing, sealing,
  torn ledger lines, and an interrupted run at every write step.

## Open questions

- Off-site copy of `history/`, possibly arranged by the SysAdmin team
  outside this tool. Is `/hgsc_software` (`sug-hnas01:/test_software/8`)
  backed up (for example by Bacula, as `/groups` is)?

## Decisions

Recorded 2026-10-23:

- History storage is sharded by bucket: immutable bases and monthly
  segments, plus a per-bucket ledger.
- Per-bucket policy (history-only, excluded) comes from an optional
  configuration file. The log and config buckets are expected to be
  history-only.
- Off-site copying is out of scope for this tool.
- The existing archive is pruned once the backfilled history verifies,
  with no separate cold copy.
- Ingestion is a step of `aos-scan` that can be turned off, and works with
  the production pattern of two runs into one output root.
- Raw catalogs are kept 14 days by default; a snapshot is held when it
  loses more than 10% and more than 1,000 rows. Both are settable on the
  command line and in the configuration file.
- History files end lines in LF.
- Each accepted snapshot is checked by re-reading its stored events and
  applying them to the previous state; a raw catalog may be pruned once
  its record exists. Full replay is verified at each monthly seal.
  (Replaces the earlier rule that the next snapshot's digest check
  sufficed: that check only showed the raw file was unchanged.)
- A held snapshot blocks its series until it is accepted or rejected.
- Locks are per bucket. Nightly ingestion skips a bucket that has
  unrecorded catalogs without sidecars, and one that backfill has locked.
- Bases after the first use `present` rows, so `initial` still means the
  start of tracking.
- Readers of data files take a shared lock, so a seal cannot delete
  files under them; `verify --clean` takes the exclusive lock.
- A failed bucket does not end a scan, but a credential error does, and
  any failure makes `aos-scan` exit non-zero. Stale `.partial` files and
  orphaned sidecars are reported, not halted on, and an admin removes
  them with `aos-history clean`.
- `last_modified` is truncated to whole seconds. Users need the date and
  perhaps the hour of a change.
- `--accept` and `--reject` name the bucket as well as the snapshot,
  since empty buckets list in under a second and can share a
  `snapshot_at`. They also resolve holds during backfill.

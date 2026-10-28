# CLAUDE.md

Notes for reviewing the user documentation in `docs/`. Their purpose is
to suppress false-positive findings in automated reviews.

## Verified: `aws s3 cp` and restored Glacier objects

`docs/s3.md` says that a `--recursive` copy skips `GLACIER` objects even after
they are restored, while a single-object copy of a restored object works.
This was checked against aws-cli 2.38.2 on 2026-10-20 and is correct. Do
not flag it unless a newer CLI behaves differently.

### Why

In `awscli/customizations/s3/fileinfo.py`, `FileInfo._is_restored()`
treats an object as restored only if its response data has a `Restore`
string containing `ongoing-request="false"`.

- A single-object copy gets that data from `HeadObject`, which returns
  `Restore`.
- A `--recursive` copy gets it from `ListObjectsV2`
  (`BucketLister.list_objects()` in `utils.py`). The CLI does not request
  `OptionalObjectAttributes=RestoreStatus`, and even that attribute would
  arrive as `RestoreStatus`, not `Restore`. Every `GLACIER` or
  `DEEP_ARCHIVE` object therefore looks unrestored.

### Experiment

A copy of `README.md` was uploaded to
`s3://TEST-BUCKET/glacier-test/README.md` with
`--storage-class GLACIER`, then restored with
`{"Days": 1, "GlacierJobParameters": {"Tier": "Expedited"}}`. The
Expedited restore completed within minutes.

| Copy | Before restore | After restore |
|---|---|---|
| Single object | Skipped, warning, exit 2 | Copied, exit 0 |
| `--recursive` | Skipped, warning, exit 2 | Skipped, warning, exit 2 |
| `--recursive --force-glacier-transfer` | Not run | Copied, exit 0 |

The skip warning, quoted in `docs/s3.md`, was:

```
warning: Skipping file s3://TEST-BUCKET/glacier-test/README.md. Object is of storage class GLACIER. Unable to perform download operations on GLACIER objects. You must restore the object to be able to perform the operation. See aws s3 download help for additional parameter options to ignore or force these transfers.
```

`head-object` on the restored object showed
`ongoing-request="false", expiry-date="Thu, 22 Oct 2026 00:00:00 GMT"`
for a restore requested on 2026-10-20 with `"Days": 1`: S3 rounds the
expiry up to the next midnight UTC.

### Re-checking

To re-check with a newer CLI, grep the installed `awscli/customizations/s3`
package for `_is_restored` and `RestoreStatus`. If the recursive lister
now requests `RestoreStatus` and `_is_restored` reads it, repeat the
experiment above and update `docs/s3.md`.

## Verified: `aws` does not read stdin in the `while read` loops

The `while IFS= read -r key; do ... done < keys.txt` loops in
`docs/s3.md` run `aws s3api head-object` and `restore-object` without
redirecting stdin. With aws-cli 2.38.2 on 2026-10-20, a four-key loop
ran each command once per key, including a missing key that failed, so
neither command consumes the loop's input. Do not flag the loops for
lacking `< /dev/null`.

## Deliberately not covered: dots in bucket names

In `docs/s3.md`, the `sed -n 's|^s3://BUCKET/||p'` command treats any
`.` in a bucket name as a regex wildcard. The maintainer has decided not
to address this, since the docs cannot cover everything. Do not flag it.

## Review scope

Notes for automated reviewers of `docs/s3-history-spec.md`. They exist
to keep reviews on target and to stop settled findings from coming
back.

### Scope

- AUDIENCE AND PURPOSE: `docs/s3-history-spec.md` is a draft design
  spec for an internal tool run nightly by one team on the cluster. Its
  reader is the implementer (a person or an AI session). It must be
  unambiguous enough to build and test from; it is not an
  implementation document.
- BAR: Report only (a) contradictions between sections; (b) rules that
  would lose or corrupt history, or let a raw catalog be pruned before
  its content is provably in the history; (c) normal-operation paths
  (nightly ingestion, backfill, hold/accept/reject, seal, prune,
  reconstruct, verify) the spec leaves undefined, so an implementer
  would have to guess. Do not report wording or style unless it is
  ambiguous.
- CRASH REQUIREMENT: The only failure case in scope is a single crash
  at any one write step. Its outcome must be at least one of:
  - no effect on other operations, and automatic recovery on the next
    successful run through idempotence; or
  - detected by the next run on its own (it refuses or reports the
    condition), requiring manual intervention such as removing a stale
    file or running with a recovery option.
  Report a write step only if a single crash there meets neither.
- OUT OF SCOPE: crashes during recovery, multiple failures, filer or
  NFS misbehavior beyond the existing `flock` note, disk-full and
  partial-write variants beyond torn ledger lines. **Why:** the
  requirement above is the bar; deeper failure combinations are a
  rabbit hole.
- OUT OF SCOPE: concurrency beyond the stated model (one nightly
  `aos-scan` of two accounts into one root, plus occasional admin
  commands under per-bucket locks), such as racing admins or clock
  skew between hosts. **Why:** rabbit hole.
- OUT OF SCOPE: S3 listing pathologies beyond truncation and mass drops
  already handled, such as listing consistency anomalies, pagination
  bugs, or keys S3 itself rejects. **Why:** rabbit hole.
- OUT OF SCOPE: recovery from damaged or corrupted history files beyond
  what the spec already states (stop and report, and the existence of
  `aos-history rebase`). Do not ask how sealing, `verify`, or pruning
  should treat history before a `rebase` base. **Why:** damage
  is a failure beyond the single-crash requirement; deeper recovery
  paths are a rabbit hole.
- LIMITED: backfill edge cases from legacy catalogs (rare key shapes,
  unusual file names such as DST-ambiguous times) need at most a
  caveat, not new rules. **Why:** they are rare, and nothing is lost,
  since rejected catalogs are never pruned.
- OUT OF SCOPE: implementation detail — exact CLI option sets, internal
  module APIs, performance tuning, and query-format internals beyond
  what "Query support" states — unless the spec contradicts itself.
  **Why:** decided during implementation, not in the spec.
- AUDIENCE AND PURPOSE (overview): `docs/s3-history-overview.md`
  introduces the spec for team members and implementers, with diagrams
  and a worked example. The spec is authoritative where they differ.
  The overview simplifies on purpose; it is not a second spec.
- BAR (overview): Report only (a) statements, diagrams, or tables that
  contradict the spec; (b) a worked example that is wrong inside itself
  (rows, events, or counts that do not follow from the steps shown);
  (c) simplifications that would mislead a reader about what the tool
  does.
- OUT OF SCOPE (overview): completeness against the spec. Do not report
  spec rules the overview omits, as long as nothing it says is wrong.
  **Why:** the spec holds the exact rules; the overview is a reading aid.
- OUT OF SCOPE (overview): Mermaid diagram style and layout, except
  syntax that will not render on GitHub. **Why:** rabbit hole.
- OUT OF SCOPE (overview): asking the operator guide to cover more
  failure scenarios or recovery steps than the spec defines. **Why:** the
  spec's crash and recovery limits above apply.
- LIMITED (overview): figures (sizes, percentages, digests) are in scope
  only where they are inconsistent within the worked example or with the
  spec's "Motivation". Do not re-derive them from production data.
  **Why:** they illustrate scale, not measure it.

### Settled

- None yet.

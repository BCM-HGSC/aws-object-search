# S3 Bucket History Compressor — Design Notes

## Purpose

Consume periodic listings ("snapshots") of an S3 bucket and emit a compact event stream that records how the bucket changed between snapshots. The stream replaces the snapshots: any snapshot can be reconstructed by replaying events up to that point.

## Scope

In scope for v1:

- Diffing consecutive snapshots of one bucket (or one bucket + prefix) on `key`.
- Emitting `initial`, `new`, `changed`, and `gone` events with the listed metadata.
- Validating snapshots before diffing.
- Reconstructing a snapshot from the stream (needed for testing).

Out of scope for v1:

- Rename detection (possible later as an annotation; see "Future").
- Fetching anything beyond the listing (HeadObject, tags, user metadata, encryption).
- S3 Event Notifications / EventBridge integration.
- Producing the snapshots themselves.

## Input: Snapshot

One snapshot is the complete result of a ListObjectsV2 pass over the bucket/prefix.

Per-object fields:

| Field | Notes |
|---|---|
| `key` | Identity. Unique within a snapshot. |
| `last_modified` | When the object's current content was written. Not changed by lifecycle transitions. |
| `size` | Bytes. |
| `storage_class` | Changes on lifecycle transition without touching `last_modified`. |
| `e_tag` | MD5 for single-part non-KMS uploads; otherwise opaque. Not a reliable content hash across copies. |
| `checksum_algorithm` | Which additional checksum(s) exist. The checksum value is **not** in the listing. |
| `checksum_type` | `FULL_OBJECT` or `COMPOSITE`. |

Per-snapshot fields (must be recorded by whatever produces snapshots):

| Field | Notes |
|---|---|
| `bucket`, `prefix` | Every snapshot in a series must share these. |
| `listing_started_at` | Snapshot timestamp used on events. |
| `listing_completed_at` | A listing is not a point-in-time image; objects can change while pagination runs. Record the window. |
| `complete` | True only if pagination finished (final page `IsTruncated=false`). |

### Snapshot validation

Reject (do not diff) a snapshot that:

- is not marked complete,
- contains duplicate keys,
- does not match the bucket/prefix of the series.

A partial listing is the main operational risk: every missing key would be emitted as a false `gone`, then a false `new` on the next run.

### Field normalization

Normalize before comparing, or spurious `changed` events will appear:

- `e_tag`: strip surrounding quotes consistently.
- `last_modified`: parse to UTC with fixed precision.
- `checksum_algorithm`: the API returns a list; sort it, and treat absent and empty the same.
- Absent optional fields (`checksum_*`): use one canonical null.

## Output: Event Stream

One row per event:

| Column | Description |
|---|---|
| `event` | `initial` \| `new` \| `changed` \| `gone` |
| `snapshot_at` | `listing_started_at` of the snapshot where the event was observed |
| `prev_snapshot_at` | `listing_started_at` of the snapshot diffed against (null for `initial`) |
| `key` | Object key |
| `last_modified`, `size`, `storage_class`, `e_tag`, `checksum_algorithm`, `checksum_type` | See below |
| `changed_fields` | For `changed` only: which fields differed |

Metadata values by event:

- `initial`, `new`, `changed`: the object's values in the current snapshot.
- `gone`: the object's last known values (from the previous snapshot).

Prior values on `changed` are not stored; they are recoverable from the stream.

### Diff semantics

Given previous snapshot P and current snapshot C:

- key in C, not in P → `new`
- key in P, not in C → `gone`
- key in both, any normalized non-key field differs → `changed`
- key in both, all fields equal → no event

The first snapshot of a series emits every key as `initial`, so a real wave of new objects is distinguishable from the start of tracking.

### Interpreting `changed_fields`

These are hints for consumers, not separate event types:

| Differing fields | Likely cause |
|---|---|
| `last_modified` + `e_tag` (often `size`) | Content overwritten |
| `last_modified` only | Same bytes rewritten (copy-onto-itself for a metadata/encryption change, or identical re-upload) |
| `storage_class` only | Lifecycle transition |
| `storage_class` + `last_modified` | Manual class change via copy-onto-itself |
| `checksum_*` + `last_modified` | Rewritten with different checksum settings |

## Core Invariant

For every snapshot S_n in a series:

```
reconstruct(events where snapshot_at <= S_n.listing_started_at) == normalize(S_n)
```

Reconstruction: start empty; apply `initial`/`new`/`changed` as upserts and `gone` as deletes, in snapshot order. This is the primary test.

## Known Limits

The stream records net differences between snapshots, not true history:

- Several overwrites of one key between snapshots appear as one `changed`.
- Delete followed by re-upload under the same key appears as `changed`.
- Create followed by delete within one interval leaves no trace.
- A rename appears as `gone` + `new` (S3 has no rename; it is copy + delete).
- `gone` time is only known to lie within the snapshot window; S3 listings carry no deletion time.
- Changes to tags, user metadata, ACLs, encryption, Object Lock, and Glacier restore status are invisible unless they cause a rewrite (which shows as `last_modified` changing).
- Versioned buckets: plain listings show only current versions; delete markers and noncurrent versions are not represented.

## Implementation Notes

- ListObjectsV2 returns keys in UTF-8 binary order. If snapshots are stored in that order, the diff is a streaming merge join with constant memory, which suits large buckets. Verify sort order on input rather than assuming it.
- The compressor needs only the previous snapshot (or the reconstructed state) to process the next one.
- Make processing idempotent per snapshot: re-running the same snapshot pair must produce identical events.

## Future (not v1)

- Rename annotation: link a `gone` and `new` in the same snapshot pair when (`e_tag`, `size`) match uniquely, with `checksum_type` as a tiebreaker. Mark as heuristic; skip ambiguous matches. Multipart copies may not preserve `e_tag`.
- Restore status via `OptionalObjectAttributes=RestoreStatus` in the listing.

## Open Questions

- Storage format and location for snapshots and the event stream (Parquet, CSV, JSONL, database table).
- Are existing snapshots already sorted by key in binary order?
- Single bucket/prefix per stream, or many streams managed by one process?
- Retention: once the invariant test passes, are the raw snapshots kept or discarded?
- Implementation language and runtime (e.g. Python on the HPC vs elsewhere).

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

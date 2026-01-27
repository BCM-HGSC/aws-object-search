# RELEASING.md

## Overview

This repository uses **annotated Git tags** as the **authoritative source of release notes**.
Releases are **GitHub‑optional**: all information needed to understand a release is contained in the tag message and the repository history.

*   Versioning: **SemVer** (`vMAJOR.MINOR.PATCH`)
*   Source of truth: **Annotated tags** (`git tag -a …`)
*   Optional: Generate `CHANGELOG.md` from tags (offline, Git‑native)

## Prerequisites

*   Git installed and configured with your name & email
*   (Optional) A GPG key configured if you prefer signed tags

```bash
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
# Optional: sign tags by default
git config --global tag.gpgSign true
```

## Versioning Rules (SemVer)

*   **MAJOR**: incompatible changes
*   **MINOR**: add functionality in a backward‑compatible manner
*   **PATCH**: backward‑compatible bug fixes

Tag names **must** be prefixed with `v`, e.g., `v1.4.0`.

## Tag Message Standard

Tags must use this structure:

    Release vX.Y.Z

    Summary:
    <1–2 sentences describing the release at a high level>

    Changes:
    - <bullet 1>
    - <bullet 2>
    - <bullet 3>

    Notes:
    - <optional compatibility notes, deprecations, schema updates>

    Commit: <auto-populated SHA>
    Date: <ISO-8601>

*   Keep bullets concise and factual.
*   Include noteworthy schema/tooling changes (e.g., SRA XML updates, FASTQ handling changes).
*   Omit a section if empty.

## Recommended Command (Git Alias)

Add this alias once to `~/.gitconfig`:

```ini
[tag]
    # Optional: enforce signed annotated tags if you use GPG
    forceSignAnnotated = true

[alias]
    release = "!f() { \
        if [ -z \"$1\" ]; then echo 'Usage: git release <version>'; exit 1; fi; \
        version=$1; \
        tmpfile=$(mktemp); \
        cat > \"$tmpfile\" <<EOF
Release $version

Summary:

Changes:

Notes:

Commit: $(git rev-parse HEAD)
Date: $(date -Iseconds)
EOF
        ${EDITOR:-vi} \"$tmpfile\"; \
        git tag -a \"$version\" -F \"$tmpfile\"; \
        rm \"$tmpfile\"; \
    }; f"
```

Usage:

```bash
git release v0.4.2
```

This opens your editor with a pre‑filled template and creates an **annotated** tag for the current commit.

## Release Checklist

1.  **Prepare the branch**
    *   Ensure `main` (or your release branch) is green and up to date
    *   Run tests / linters / pipeline checks

2.  **Select the version**
    *   Decide MAJOR/MINOR/PATCH per SemVer

3.  **Create the tag**
    ```bash
    git release vX.Y.Z
    ```

4.  **Verify the tag**
    ```bash
    git show vX.Y.Z
    ```

5.  **Push the tag**
    ```bash
    git push origin vX.Y.Z
    ```

6.  **(Optional) Update a generated CHANGELOG**
    ```bash
    git tag --sort=creatordate --format="## %(tag)%0a%0a%(contents)" > CHANGELOG.md
    git add CHANGELOG.md
    git commit -m "Update CHANGELOG for vX.Y.Z"
    git push
    ```

> GitHub Release pages are optional: if you want one, create it from the tag and paste the same content. Tags remain the source of truth.

## Examples

**Example: v0.4.2**

    Release v0.4.2

    Summary:
    Improved SRA XML generation stability and added support for new 2026 BioSample attributes.

    Changes:
    - Added fallback handling for missing BioProject accession fields
    - Updated XML schema templates to support NCBI's January 2026 update
    - Fixed FASTQ basename parsing when sample IDs contain periods
    - Improved logging around validation failures

    Notes:
    - Deprecation: legacy --schema-path flag will be removed in v0.5.0

    Commit: 1a2b3c4d5e6f7g8h9i0j
    Date: 2026-01-26T10:45:12-06:00

## Optional Validation (Lightweight)

If you want gentle guardrails, keep creating tags via `git release` (above).
You can also add a simple checker script to ensure:

*   `Summary:` is non‑empty
*   At least one `Changes:` bullet exists
*   Tag name matches `^v[0-9]+\.[0-9]+\.[0-9]+$`

Place this script at `scripts/validate-tag.sh` and run it as needed:

```bash
#!/usr/bin/env bash
set -euo pipefail

tag="${1:-}"
if [[ -z "$tag" ]]; then
  echo "Usage: $0 <tag>"; exit 2
fi

if ! git rev-parse "$tag" >/dev/null 2>&1; then
  echo "Error: tag '$tag' not found"; exit 1
fi

msg="$(git for-each-ref "refs/tags/$tag" --format='%(contents)')"

require_section() {
  local header="$1"
  if ! grep -q "^$header" <<<"$msg"; then
    echo "Missing header: $header"; exit 1
  fi
}

require_nonempty_after() {
  local header="$1"
  awk -v h="$header" '
    BEGIN{found=0; nonempty=0}
    $0 ~ "^"h {found=1; next}
    found==1 {
      if ($0 ~ /^[A-Za-z].+:/) exit
      if ($0 ~ /^[-*] /) nonempty=1
      if ($0 ~ /[[:alnum:]]/ && $0 !~ /^Commit:/ && $0 !~ /^Date:/) nonempty=1
    }
    END{exit (found && nonempty) ? 0 : 1}
  ' <<<"$msg" || { echo "Section '$header' must have content"; exit 1; }
}

require_section "Summary:"
require_nonempty_after "Summary:"
require_section "Changes:"
require_nonempty_after "Changes:"

echo "Tag '$tag' looks good."
```

Run after creating a tag:

```bash
bash scripts/validate-tag.sh vX.Y.Z
```

## Generating a CHANGELOG from Tags (Optional)

If you want a browsable `CHANGELOG.md` but keep tags as the source, generate it from tags:

```bash
git tag --sort=creatordate --format="## %(tag)%0a%0a%(contents)" > CHANGELOG.md
```

For a script with a header and date extraction:

```bash
#!/usr/bin/env bash
set -euo pipefail
{
  echo "# Changelog"
  echo
  git for-each-ref --sort=creatordate --format='%(tag) %(creatordate:iso8601)' refs/tags \
  | while read -r tag date; do
      printf "## %s (%s)\n\n" "$tag" "$date"
      git for-each-ref "refs/tags/$tag" --format='%(contents)'
      echo
    done
} > CHANGELOG.md
```

Commit the updated file:

```bash
git add CHANGELOG.md
git commit -m "Update CHANGELOG"
```

## Troubleshooting

*   **Wrong commit tagged**
    Create a corrected tag pointing to a specific commit:
    ```bash
    git tag -a vX.Y.Z <commit-sha> -m "Release vX.Y.Z …"
    git tag -d vX.Y.Z && git push --delete origin vX.Y.Z   # if already pushed
    git tag -a vX.Y.Z <correct-sha> -m "…"
    git push origin vX.Y.Z
    ```

*   **Need to tweak the message after pushing**
    Tags are immutable by convention. If you must fix it:
    ```bash
    git tag -d vX.Y.Z
    git push --delete origin vX.Y.Z
    # recreate with corrected message
    git tag -a vX.Y.Z -m "…"
    git push origin vX.Y.Z
    ```

*   **Signed tag fails (GPG issues)**
    *   Ensure `gpg` can find your key and that `GPG_TTY` is set:
        ```bash
        export GPG_TTY=$(tty)
        ```
    *   Or temporarily bypass signing:
        ```bash
        git -c tag.gpgSign=false tag -a vX.Y.Z -m "…"
        ```

## Rationale

*   **Self-contained**: Tag messages include concise release notes and metadata (commit SHA, date).
*   **Portable**: Works offline and in Git mirrors; GitHub Releases are optional.
*   **Low‑ceremony**: Minimal boilerplate, easy to maintain for small projects.
*   **Traceable**: `git show vX.Y.Z` reveals the full release context.

## Ownership

Release tags are created by the repository maintainers (you or one teammate).
If the process changes, update this document and (optionally) the alias script.

## Appendix: Quick Reference

```bash
# Create a release tag with template
git release v1.2.3

# Inspect tag contents
git show v1.2.3

# Push the tag
git push origin v1.2.3

# Generate CHANGELOG from tags (optional)
git tag --sort=creatordate --format="## %(tag)%0a%0a%(contents)" > CHANGELOG.md
```

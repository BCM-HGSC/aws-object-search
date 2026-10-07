---
name: release
description: Generate release notes, tag, and publish a GitHub release for a given version
argument-hint: VERSION
disable-model-invocation: true
---

# Release

Create a new release with auto-generated release notes.

## Arguments
- `$ARGUMENTS` - The version number (e.g., `1.0.0-rc4`, `1.0.0`)

## Instructions

You are creating a release for version `$ARGUMENTS`. Follow these steps:

### Step 1: Gather Context

Run the context gathering script to get git history since the last release:

```bash
./scripts/release-notes-context.sh $ARGUMENTS
```

### Step 2: Generate Release Notes

Based on the git history, generate release notes following this format:

```markdown
# Release Notes: aws-object-search v{VERSION}

## Overview
{1-2 sentences summarizing the release}

## What's New

### {Feature Name} (#{PR_NUMBER})
{Description}

- **{sub-feature}**: {details}

## Bug Fixes

### {Bug Fix Name} (#{PR_NUMBER})
{Description of what was fixed}

Fixes #{ISSUE_NUMBER}.

## Infrastructure & Internal Improvements

### {Category} (#{PR_NUMBER})
- {bullet points}

## Testing
All changes have been thoroughly tested:
- {test count}+ test cases pass
- Integration tests verify end-to-end functionality
- Code quality checks pass via ruff and pre-commit

## Issues Resolved
- #{NUMBER}: {description}
```

Guidelines for generating notes:
- Group commits by their PR (look for "PR #XX" in merge commits)
- Categorize changes: features go in "What's New", bugs in "Bug Fixes", tooling/docs in "Infrastructure"
- Reference issue numbers where commits mention "Fixes #XX" or "See #XX"
- Keep descriptions concise but informative
- Match the tone and style of previous release notes

### Step 3: Get Approval

Write the generated release notes to `release-notes/v$ARGUMENTS.md`, then ask the user to approve:

Use AskUserQuestion with:
- Question: "I've generated release notes at release-notes/v$ARGUMENTS.md. Please review and approve, or select 'Edit' to make changes."
- Options: "Approve" (proceed with release), "Edit" (user will edit the file), "Cancel" (abort release)

If the user selects "Edit", wait for them to confirm they're done editing, then re-read the file.

### Step 4: Finalize Release

Once approved, run the finalization script:

```bash
./scripts/release-finalize.sh $ARGUMENTS release-notes/v$ARGUMENTS.md
```

This script will:
1. Refuse to start unless on `main` (or `$RELEASE_BRANCH`), clean, and in sync with origin
2. Update version in pyproject.toml and uv.lock
3. Commit the release notes and version bump
4. Create an annotated git tag
5. Push the release commit and tag to origin atomically
6. Create a GitHub release (pre-release if version contains -rc, -alpha, or -beta)
7. Bump version to +dev in pyproject.toml and uv.lock
8. Commit and push the +dev version

If the script stops at its preflight checks, report the error to the user
and stop. Do not try to work around it (switching branches, stashing,
pulling) without the user's direction.

### Step 5: Report Completion

After the script completes, report the GitHub release URL to the user.

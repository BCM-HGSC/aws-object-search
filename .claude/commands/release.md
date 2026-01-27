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
1. Update version in pyproject.toml
2. Commit the release notes and version bump
3. Create an annotated git tag
4. Push the tag to origin
5. Create a GitHub release (pre-release if version contains -rc, -alpha, or -beta)
6. Bump version to +dev
7. Commit and push the +dev version

### Step 5: Report Completion

After the script completes, report the GitHub release URL to the user.

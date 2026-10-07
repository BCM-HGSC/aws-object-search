#!/usr/bin/env bash
# Gathers git history context for release notes generation
# Usage: release-notes-context.sh [NEW_VERSION]
#
# If NEW_VERSION is provided, determines the previous tag automatically.
# Outputs formatted git log and example release notes for Claude to use.

set -euo pipefail

NEW_VERSION="${1:-}"

# Get the most recent tag
PREV_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")

if [[ -z "$PREV_TAG" ]]; then
    echo "Error: No previous tag found" >&2
    exit 1
fi

RANGE="${PREV_TAG}..HEAD"

echo "=== Release Context ==="
echo "Previous tag: $PREV_TAG"
echo "New version: ${NEW_VERSION:-TBD}"
echo "Commit range: $RANGE"
echo ""

echo "=== Commits Since $PREV_TAG ==="
git log --format="%H %s%n%b---" "$RANGE"
echo ""

echo "=== Previous Release Notes (for format reference) ==="
PREV_NOTES="release-notes/${PREV_TAG}.md"
if [[ -f "$PREV_NOTES" ]]; then
    cat "$PREV_NOTES"
else
    echo "(No previous release notes file found at $PREV_NOTES)"
fi

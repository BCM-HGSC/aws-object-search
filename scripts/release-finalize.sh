#!/usr/bin/env bash
# Finalizes a release after release notes have been approved
# Usage: release-finalize.sh VERSION RELEASE_NOTES_FILE
#
# Must be run on the release branch (main, or $RELEASE_BRANCH) with a
# clean working tree, in sync with origin. Checks these before starting.
#
# Performs all deterministic release steps:
# 1. Updates version in pyproject.toml
# 2. Commits release notes and version bump
# 3. Creates annotated git tag
# 4. Pushes tag to origin
# 5. Creates GitHub pre-release
# 6. Bumps version to +dev
# 7. Commits and pushes

set -euo pipefail

VERSION="${1:-}"
NOTES_FILE="${2:-}"

if [[ -z "$VERSION" ]]; then
    echo "Error: VERSION is required" >&2
    echo "Usage: release-finalize.sh VERSION RELEASE_NOTES_FILE" >&2
    exit 1
fi

if [[ -z "$NOTES_FILE" ]] || [[ ! -f "$NOTES_FILE" ]]; then
    echo "Error: RELEASE_NOTES_FILE is required and must exist" >&2
    echo "Usage: release-finalize.sh VERSION RELEASE_NOTES_FILE" >&2
    exit 1
fi

TAG="v${VERSION}"
NOTES_DEST="release-notes/${TAG}.md"
RELEASE_BRANCH="${RELEASE_BRANCH:-main}"

# Preflight: verify repository state before changing anything
fail() {
    echo "Error: $*" >&2
    exit 1
}

CURRENT_BRANCH=$(git branch --show-current)
if [[ "$CURRENT_BRANCH" != "$RELEASE_BRANCH" ]]; then
    fail "on branch '${CURRENT_BRANCH:-detached HEAD}', expected '$RELEASE_BRANCH'" \
        "(set RELEASE_BRANCH to override)"
fi

# Tracked changes other than the release notes would leak into the commit
DIRTY=$(git status --porcelain --untracked-files=no | grep -v -F " $NOTES_DEST" || true)
if [[ -n "$DIRTY" ]]; then
    fail "working tree has uncommitted changes:"$'\n'"$DIRTY"
fi

if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
    fail "tag $TAG already exists"
fi

git fetch --quiet origin "$RELEASE_BRANCH"
if [[ "$(git rev-parse HEAD)" != "$(git rev-parse "origin/$RELEASE_BRANCH")" ]]; then
    fail "$RELEASE_BRANCH is not in sync with origin/$RELEASE_BRANCH"
fi

echo "=== Release Finalization for $TAG ==="
echo ""

# Step 1: Copy release notes to final location if not already there
if [[ "$NOTES_FILE" != "$NOTES_DEST" ]]; then
    echo "Step 1: Copying release notes to $NOTES_DEST"
    mkdir -p release-notes
    cp "$NOTES_FILE" "$NOTES_DEST"
else
    echo "Step 1: Release notes already at $NOTES_DEST"
fi

# Step 2: Update version in pyproject.toml
echo "Step 2: Updating version to $VERSION in pyproject.toml"
sed -i.bak "s/^version = \".*\"/version = \"$VERSION\"/" pyproject.toml
rm -f pyproject.toml.bak

# Step 3: Commit release notes and version bump
echo "Step 3: Committing release notes and version bump"
git add "$NOTES_DEST" pyproject.toml
git commit -m "Bump version to $VERSION"

# Step 4: Create annotated tag
echo "Step 4: Creating annotated tag $TAG"
COMMIT_SHA=$(git rev-parse HEAD)
COMMIT_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# Generate tag message from release notes
# Extract summary (first paragraph after Overview header)
SUMMARY=$(sed -n '/^## Overview/,/^##/{/^## Overview/d;/^##/d;p;}' "$NOTES_DEST" | tr '\n' ' ' | sed 's/  */ /g' | sed 's/^ *//;s/ *$//')

# Extract changes (items from What's New and Bug Fixes sections)
CHANGES=$(grep -E "^### |^- \*\*|^Fixed |^Added " "$NOTES_DEST" | head -10 | sed 's/^### /- /' | sed 's/^\*\*/- **/')

git tag -a "$TAG" -m "$(cat <<EOF
Release $TAG

Summary:
$SUMMARY

Changes:
$CHANGES

Commit: $COMMIT_SHA
Date: $COMMIT_DATE
EOF
)"

# Step 5: Push tag to origin
echo "Step 5: Pushing tag to origin"
git push origin "$TAG"

# Step 6: Create GitHub pre-release
echo "Step 6: Creating GitHub pre-release"
if [[ "$VERSION" == *"-rc"* ]] || [[ "$VERSION" == *"-alpha"* ]] || [[ "$VERSION" == *"-beta"* ]]; then
    gh release create "$TAG" --prerelease --title "$TAG" --notes-file "$NOTES_DEST"
else
    gh release create "$TAG" --title "$TAG" --notes-file "$NOTES_DEST"
fi

# Step 7: Bump version to +dev
echo "Step 7: Bumping version to ${VERSION}+dev"
sed -i.bak "s/^version = \".*\"/version = \"${VERSION}+dev\"/" pyproject.toml
rm -f pyproject.toml.bak

# Step 8: Commit and push
echo "Step 8: Committing and pushing +dev version"
git add pyproject.toml
git commit -m "Bump version to ${VERSION}+dev"
git push origin "$RELEASE_BRANCH"

echo ""
echo "=== Release $TAG Complete ==="
echo "GitHub Release: https://github.com/$(gh repo view --json nameWithOwner -q .nameWithOwner)/releases/tag/$TAG"

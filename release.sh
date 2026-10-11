#!/bin/bash

# release.sh - Automated release tag script for MonsterMQ Edge
# Usage:
#   ./release.sh               # Auto-increments patch version (e.g. 0.1.14 -> 0.1.15)
#   ./release.sh 0.2.0         # Sets explicit version 0.2.0
#   ./release.sh -r, --rebase  # Re-tags current version to latest commit (removes tag locally & remotely)
#   ./release.sh -h, --help    # Shows this help message

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -f "version.txt" ]; then
    echo -e "${RED}Error: version.txt not found${NC}"
    exit 1
fi

CURRENT_VERSION=$(head -n 1 version.txt | tr -d '\n' | tr -d '\r')
BASE_VERSION=$(echo "$CURRENT_VERSION" | cut -d'+' -f1)

# Parse BASE_VERSION into MAJOR, MINOR, PATCH, and optional PRERELEASE
if [[ "$BASE_VERSION" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)(-([a-zA-Z0-9.-]+))?$ ]]; then
    MAJOR="${BASH_REMATCH[1]}"
    MINOR="${BASH_REMATCH[2]}"
    PATCH="${BASH_REMATCH[3]}"
    PRERELEASE="${BASH_REMATCH[5]}"
else
    echo -e "${RED}Error: Invalid version format in version.txt. Expected format: X.Y.Z or X.Y.Z-dev${NC}"
    echo -e "${RED}Current content: '$CURRENT_VERSION'${NC}"
    exit 1
fi

usage() {
    echo -e "${BOLD}MonsterMQ Edge Release Script${NC}"
    echo ""
    echo "Usage: $0 [options | version]"
    echo ""
    echo "Options:"
    echo "  (no arguments)            Auto-increment patch version or finalize active dev version"
    echo "  <version>                 Set explicit version (e.g. 0.2.0)"
    echo "  --dev, --next [version]   Advance to next development version (e.g. 0.1.16-dev)"
    echo "  -r, --rebase, --retag     Rebase current version (v${BASE_VERSION}) to HEAD commit:"
    echo "                            removes the tag locally and remotely, then creates and pushes it on HEAD"
    echo "  -h, --help                Show this help message"
    echo ""
    exit 0
}

if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    usage
fi

if [ "$1" = "--dev" ] || [ "$1" = "--next" ]; then
    if [ -n "${2:-}" ]; then
        NEXT_DEV="$2"
        if [[ ! "$NEXT_DEV" =~ -dev$ ]]; then
            NEXT_DEV="${NEXT_DEV}-dev"
        fi
    else
        if [ -n "$PRERELEASE" ]; then
            NEXT_DEV="${MAJOR}.${MINOR}.${PATCH}-${PRERELEASE}"
            echo -e "${YELLOW}Already on a development version: ${NEXT_DEV}${NC}"
            exit 0
        else
            NEXT_DEV="${MAJOR}.${MINOR}.$((PATCH + 1))-dev"
        fi
    fi
    echo -e "${GREEN}=== Advancing to Development Version: ${NEXT_DEV} ===${NC}"
    echo "$NEXT_DEV" > version.txt
    git add version.txt
    git commit -m "chore(version): prepare next version ${NEXT_DEV}" || {
        echo -e "${YELLOW}No new changes to commit${NC}"
    }
    git push origin HEAD
    echo -e "${GREEN}✓ Pushed development version ${NEXT_DEV} to origin/main${NC}"
    exit 0
fi

MODE="bump"
if [ "$1" = "-r" ] || [ "$1" = "--rebase" ] || [ "$1" = "--retag" ] || [ "$1" = "--readjust" ]; then
    MODE="rebase"
    if [ -n "$2" ]; then
        NEW_VERSION="$2"
    else
        NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
    fi
elif [ -n "$1" ]; then
    MODE="explicit"
    NEW_VERSION="$1"
else
    if [ -n "$PRERELEASE" ]; then
        MODE="finalize"
        NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
    else
        MODE="bump"
        NEW_PATCH=$((PATCH + 1))
        NEW_VERSION="${MAJOR}.${MINOR}.${NEW_PATCH}"
    fi
fi

GIT_SHA=$(git rev-parse --short HEAD)

echo -e "${GREEN}=== MonsterMQ Edge Release Script ===${NC}"
echo -e "${YELLOW}Current version : ${BASE_VERSION}${NC}"
if [ "$MODE" = "rebase" ]; then
    echo -e "${BLUE}Mode            : Rebase tag v${NEW_VERSION} to HEAD (${GIT_SHA})${NC}"
elif [ "$MODE" = "finalize" ]; then
    echo -e "${GREEN}Finalize dev    : ${BASE_VERSION} -> ${NEW_VERSION}${NC}"
else
    echo -e "${GREEN}New version     : ${NEW_VERSION}${NC}"
fi
echo -e "${GREEN}Git SHA         : ${GIT_SHA}${NC}"

if ! git diff-index --quiet HEAD --; then
    echo -e "${YELLOW}Warning: You have uncommitted changes${NC}"
    read -p "Do you want to continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${RED}Release cancelled${NC}"
        exit 1
    fi
fi

TAG_NAME="v${NEW_VERSION}"

if [ "$MODE" = "rebase" ]; then
    # 1. Delete local tag if it exists
    if git rev-parse "${TAG_NAME}" >/dev/null 2>&1; then
        echo -e "${YELLOW}Deleting local tag ${TAG_NAME}...${NC}"
        git tag -d "${TAG_NAME}"
        echo -e "${GREEN}✓ Deleted local tag ${TAG_NAME}${NC}"
    fi

    # 2. Delete remote tag if it exists on origin
    if git ls-remote --tags origin "${TAG_NAME}" | grep -q "${TAG_NAME}"; then
        echo -e "${YELLOW}Deleting remote tag ${TAG_NAME} on origin...${NC}"
        git push origin --delete "${TAG_NAME}" 2>/dev/null || git push origin ":refs/tags/${TAG_NAME}"
        echo -e "${GREEN}✓ Deleted remote tag ${TAG_NAME} on origin${NC}"
    fi
else
    if git rev-parse "${TAG_NAME}" >/dev/null 2>&1; then
        echo -e "${RED}Error: Tag ${TAG_NAME} already exists${NC}"
        echo -e "To move or rebase this tag to HEAD, run:"
        echo -e "  ${YELLOW}./release.sh --rebase${NC}"
        exit 1
    fi
fi

echo "$NEW_VERSION" > version.txt
echo -e "${GREEN}✓ Updated version.txt to ${NEW_VERSION}${NC}"

RELEASE_NOTES_FILE="releases/${TAG_NAME}.txt"
mkdir -p releases
echo "Release ${TAG_NAME}" > "$RELEASE_NOTES_FILE"
echo "Built from commit: ${GIT_SHA}" >> "$RELEASE_NOTES_FILE"
echo "Date: $(date '+%Y-%m-%d %H:%M:%S')" >> "$RELEASE_NOTES_FILE"
echo "" >> "$RELEASE_NOTES_FILE"

if [ "$MODE" = "rebase" ]; then
    PREV_TAG=$(git tag --sort=-v:refname 2>/dev/null | grep -v "^${TAG_NAME}$" | head -n 1 || echo "")
    if [ -n "$PREV_TAG" ]; then
        echo "Changes since ${PREV_TAG}:" >> "$RELEASE_NOTES_FILE"
        echo "---" >> "$RELEASE_NOTES_FILE"
        git log "${PREV_TAG}..HEAD" --oneline >> "$RELEASE_NOTES_FILE"
    else
        echo "Changes:" >> "$RELEASE_NOTES_FILE"
        echo "---" >> "$RELEASE_NOTES_FILE"
        git log --oneline -n 25 >> "$RELEASE_NOTES_FILE"
    fi
else
    echo "Changes since v${BASE_VERSION}:" >> "$RELEASE_NOTES_FILE"
    echo "---" >> "$RELEASE_NOTES_FILE"
    LAST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")
    if [ -n "$LAST_TAG" ]; then
        git log "${LAST_TAG}..HEAD" --oneline >> "$RELEASE_NOTES_FILE"
    else
        echo "Initial release" >> "$RELEASE_NOTES_FILE"
    fi
fi

echo -e "${GREEN}✓ Created release notes: ${RELEASE_NOTES_FILE}${NC}"

git add version.txt "$RELEASE_NOTES_FILE"
if [ "$MODE" = "rebase" ]; then
    COMMIT_MSG="chore(release): rebase version.txt and release notes for ${TAG_NAME}"
else
    COMMIT_MSG="Bump version to ${NEW_VERSION}"
fi

git commit -m "$COMMIT_MSG" || {
    echo -e "${YELLOW}No changes to commit${NC}"
}

echo -e "${YELLOW}Creating tag ${TAG_NAME}...${NC}"
git tag -a "${TAG_NAME}" -m "Release version ${NEW_VERSION}"
echo -e "${GREEN}✓ Created tag ${TAG_NAME}${NC}"

echo -e "${YELLOW}Pushing commits and tags...${NC}"
git push origin HEAD
git push origin --tags
echo -e "${GREEN}✓ Pushed commits and tags${NC}"

echo ""
echo -e "${GREEN}=== Release Tag Complete ===${NC}"
echo -e "${GREEN}Version ${NEW_VERSION} tagged successfully on $(git rev-parse --short HEAD).${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Build artifacts locally : ./build.sh"
echo "  2. Publish release assets  : ./publish.sh"
echo "  3. Or build & publish      : ./build.sh --publish"


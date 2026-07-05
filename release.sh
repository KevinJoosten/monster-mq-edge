#!/bin/bash

# release.sh - Automated release script for MonsterMQ Edge
# This script:
# 1. Reads version from version.txt
# 2. Increments the patch version
# 3. Checks for uncommitted changes in both parent and submodule repos
# 4. Updates version.txt to the new clean version
# 5. Writes release notes
# 6. Commits changes
# 7. Creates git tags on both parent and mochi-mqtt-server submodule repos
# 8. Prints pushing instructions

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== MonsterMQ Edge Release Script ===${NC}"

# Check if version.txt exists
if [ ! -f "version.txt" ]; then
    echo -e "${RED}Error: version.txt not found${NC}"
    exit 1
fi

# Read current version from version.txt
CURRENT_VERSION=$(head -n 1 version.txt | tr -d '\n' | tr -d '\r')

# Extract base version (without git SHA if present)
BASE_VERSION=$(echo "$CURRENT_VERSION" | cut -d'+' -f1)

# Parse version components
IFS='.' read -r MAJOR MINOR PATCH <<< "$BASE_VERSION"

# Validate version components
if [ -z "$MAJOR" ] || [ -z "$MINOR" ] || [ -z "$PATCH" ]; then
    echo -e "${RED}Error: Invalid version format in version.txt. Expected format: X.Y.Z${NC}"
    echo -e "${RED}Current content: '$CURRENT_VERSION'${NC}"
    exit 1
fi

# Increment patch version
NEW_PATCH=$((PATCH + 1))
NEW_VERSION="${MAJOR}.${MINOR}.${NEW_PATCH}"

# Get current git SHA
GIT_SHA=$(git rev-parse --short HEAD)

echo -e "${YELLOW}Current version: ${BASE_VERSION}${NC}"
echo -e "${GREEN}New version: ${NEW_VERSION}${NC}"
echo -e "${GREEN}Git SHA: ${GIT_SHA}${NC}"

# Check for uncommitted changes in parent repository
if ! git diff-index --quiet HEAD --; then
    echo -e "${YELLOW}Warning: You have uncommitted changes in parent repo${NC}"
    read -p "Do you want to continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${RED}Release cancelled${NC}"
        exit 1
    fi
fi

# Check for uncommitted changes in mochi-mqtt-server submodule
if ! git -C mochi-mqtt-server diff-index --quiet HEAD --; then
    echo -e "${YELLOW}Warning: You have uncommitted changes in mochi-mqtt-server submodule${NC}"
    read -p "Do you want to continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${RED}Release cancelled${NC}"
        exit 1
    fi
fi

# Check if parent tag already exists
if git rev-parse "v${NEW_VERSION}" >/dev/null 2>&1; then
    echo -e "${RED}Error: Parent tag v${NEW_VERSION} already exists${NC}"
    echo -e "${YELLOW}Please manually update version.txt if you need a different version${NC}"
    exit 1
fi

# Check if submodule tag already exists
if git -C mochi-mqtt-server rev-parse "v${NEW_VERSION}" >/dev/null 2>&1; then
    echo -e "${RED}Error: Submodule tag v${NEW_VERSION} already exists${NC}"
    exit 1
fi

# Update version.txt with the new clean version (no SHA suffix for standard semver)
echo "$NEW_VERSION" > version.txt
echo -e "${GREEN}✓ Updated version.txt to ${NEW_VERSION}${NC}"

# Create release notes file
RELEASE_NOTES_FILE="releases/v${NEW_VERSION}.txt"
mkdir -p releases
echo "Release v${NEW_VERSION}" > "$RELEASE_NOTES_FILE"
echo "Built from commit: ${GIT_SHA}" >> "$RELEASE_NOTES_FILE"
echo "Date: $(date '+%Y-%m-%d %H:%M:%S')" >> "$RELEASE_NOTES_FILE"
echo "" >> "$RELEASE_NOTES_FILE"
echo "Changes since v${BASE_VERSION}:" >> "$RELEASE_NOTES_FILE"
echo "---" >> "$RELEASE_NOTES_FILE"

# Get commit messages since last tag
LAST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")
if [ -n "$LAST_TAG" ]; then
    git log "${LAST_TAG}..HEAD" --oneline >> "$RELEASE_NOTES_FILE"
else
    echo "Initial release" >> "$RELEASE_NOTES_FILE"
fi

echo -e "${GREEN}✓ Created release notes: ${RELEASE_NOTES_FILE}${NC}"

# Add version.txt and release notes to git and commit the version bump
git add version.txt "$RELEASE_NOTES_FILE"
git commit -m "Bump version to ${NEW_VERSION}" || {
    echo -e "${YELLOW}No changes to commit${NC}"
}

# Create git tag on parent repo
echo -e "${YELLOW}Creating parent tag v${NEW_VERSION}...${NC}"
git tag -a "v${NEW_VERSION}" -m "Release version ${NEW_VERSION}"
echo -e "${GREEN}✓ Created parent tag v${NEW_VERSION}${NC}"

# Create git tag on mochi-mqtt-server submodule repo
echo -e "${YELLOW}Creating submodule tag v${NEW_VERSION}...${NC}"
git -C mochi-mqtt-server tag -a "v${NEW_VERSION}" -m "Release version ${NEW_VERSION}"
echo -e "${GREEN}✓ Created submodule tag v${NEW_VERSION}${NC}"

echo ""
echo -e "${GREEN}=== Release Complete ===${NC}"
echo -e "${GREEN}Version ${NEW_VERSION} has been tagged in both repositories.${NC}"
echo ""
echo -e "${YELLOW}Next steps to push commits and tags to remote:${NC}"
echo "  1. Push parent commits and tag:"
PARENT_BRANCH=$(git branch --show-current)
echo "     git push origin ${PARENT_BRANCH}"
echo "     git push origin v${NEW_VERSION}"
echo "  2. Push submodule commits and tag:"
SUBMODULE_BRANCH=$(git -C mochi-mqtt-server branch --show-current)
echo "     git -C mochi-mqtt-server push origin ${SUBMODULE_BRANCH}"
echo "     git -C mochi-mqtt-server push origin v${NEW_VERSION}"
echo ""

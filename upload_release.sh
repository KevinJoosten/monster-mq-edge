#!/bin/bash

# upload_release.sh - Build and upload Debian packages to GitHub Release
#
# This script:
# 1. Reads the current version from version.txt
# 2. Verifies the github CLI (gh) is installed and authenticated
# 3. Verifies that the required .deb packages exist (or builds them)
# 4. Creates a GitHub release if it does not exist (using releases/v<VERSION>.txt)
# 5. Uploads the .deb packages to the GitHub release

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== MonsterMQ Edge Release Asset Uploader ===${NC}"

# 1. Read version from version.txt
if [ ! -f "version.txt" ]; then
    echo -e "${RED}Error: version.txt not found${NC}"
    exit 1
fi
VERSION=$(head -n 1 version.txt | tr -d '\n' | tr -d '\r')
TAG="v${VERSION}"
echo -e "${YELLOW}Target version: ${VERSION} (${TAG})${NC}"

# 2. Check if gh CLI is installed
if ! command -v gh &> /dev/null; then
    echo -e "${RED}Error: GitHub CLI (gh) is not installed.${NC}"
    echo -e "${YELLOW}Please install it first (e.g. 'brew install gh' or 'apt install gh')${NC}"
    exit 1
fi

# 3. Check if gh is authenticated
if ! gh auth status &> /dev/null; then
    echo -e "${RED}Error: GitHub CLI (gh) is not authenticated.${NC}"
    echo -e "${YELLOW}Please run 'gh auth login' to authenticate.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ GitHub CLI authenticated successfully${NC}"

# 4. Check for .deb packages or build them
DEB_ARM64="bin/monstermq-edge_${VERSION}_arm64.deb"
DEB_ARMHF="bin/monstermq-edge_${VERSION}_armhf.deb"
DEB_AMD64="bin/monstermq-edge_${VERSION}_amd64.deb"

BUILD_NEEDED=false
if [ ! -f "$DEB_ARM64" ] || [ ! -f "$DEB_ARMHF" ] || [ ! -f "$DEB_AMD64" ]; then
    BUILD_NEEDED=true
fi

if [ "$BUILD_NEEDED" = true ]; then
    echo -e "${YELLOW}One or more Debian packages are missing in bin/. Building them now...${NC}"
    make deb-all
else
    echo -e "${GREEN}✓ Found all Debian packages in bin/${NC}"
fi

# Double check that building succeeded
if [ ! -f "$DEB_ARM64" ] || [ ! -f "$DEB_ARMHF" ] || [ ! -f "$DEB_AMD64" ]; then
    echo -e "${RED}Error: Failed to locate .deb files even after build. Aborting.${NC}"
    exit 1
fi

# 5. Check if local git tag exists
if ! git rev-parse "$TAG" >/dev/null 2>&1; then
    echo -e "${RED}Error: Local git tag ${TAG} does not exist.${NC}"
    echo -e "${YELLOW}Please run './release.sh' first to tag the release.${NC}"
    exit 1
fi

# 6. Verify tag is pushed to remote (or offer to push it)
echo -e "${YELLOW}Verifying if tag is pushed to remote...${NC}"
if ! git ls-remote --tags origin "$TAG" | grep -q "$TAG"; then
    echo -e "${YELLOW}Warning: Tag ${TAG} is not pushed to the remote repository yet.${NC}"
    read -p "Would you like to push the current branch and tag to origin now? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        CURRENT_BRANCH=$(git branch --show-current)
        echo "Pushing branch ${CURRENT_BRANCH} and tag ${TAG}..."
        git push origin "$CURRENT_BRANCH"
        git push origin "$TAG"
        
        # Also push submodule if needed
        SUBMODULE_BRANCH=$(git -C mochi-mqtt-server branch --show-current)
        echo "Pushing submodule branch ${SUBMODULE_BRANCH} and tag ${TAG}..."
        git -C mochi-mqtt-server push origin "$SUBMODULE_BRANCH"
        git -C mochi-mqtt-server push origin "$TAG"
    else
        echo -e "${RED}Error: Tag must be pushed to remote before creating GitHub release.${NC}"
        exit 1
    fi
fi
echo -e "${GREEN}✓ Tag verified on remote repository${NC}"

# 7. Check if GitHub release exists, create if not
echo -e "${YELLOW}Checking if GitHub release exists for ${TAG}...${NC}"
RELEASE_EXISTS=true
if ! gh release view "$TAG" &> /dev/null; then
    RELEASE_EXISTS=false
fi

if [ "$RELEASE_EXISTS" = false ]; then
    echo -e "${YELLOW}GitHub release does not exist. Creating release ${TAG}...${NC}"
    
    # Locate release notes notes file
    RELEASE_NOTES="releases/v${VERSION}.txt"
    if [ -f "$RELEASE_NOTES" ]; then
        gh release create "$TAG" --title "Release ${VERSION}" --notes-file "$RELEASE_NOTES"
    else
        gh release create "$TAG" --title "Release ${VERSION}" --notes "Release version ${VERSION}"
    fi
    echo -e "${GREEN}✓ GitHub release created${NC}"
else
    echo -e "${GREEN}✓ GitHub release already exists${NC}"
fi

# 8. Upload assets
echo -e "${YELLOW}Uploading Debian packages to GitHub release...${NC}"
gh release upload "$TAG" "$DEB_ARM64" "$DEB_ARMHF" "$DEB_AMD64" --clobber
echo -e "${GREEN}✓ Upload complete!${NC}"

echo ""
echo -e "${GREEN}=== Release Assets Successfully Uploaded ===${NC}"
gh release view "$TAG"
echo ""

#!/bin/bash

# publish.sh - Upload release assets to GitHub Release and push multi-arch Docker images to Docker Hub for MonsterMQ Edge
#
# Usage:
#   ./publish.sh --all        Publish Debian packages to GitHub Release and Docker Hub images
#   ./publish.sh --github     Upload Debian packages to GitHub Release only
#   ./publish.sh --docker     Build and push Docker images to Docker Hub only
#   ./publish.sh --testing, -t Publish Docker image with label "testing" to Docker Hub only
#   ./publish.sh -y           Auto-confirm publishing (non-interactive)

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -f "version.txt" ]; then
    echo -e "${RED}Error: version.txt not found${NC}"
    exit 1
fi

VERSION=$(head -n 1 version.txt | tr -d '\n' | tr -d '\r')
TAG="v${VERSION}"

IS_DEV=false
if [[ "$VERSION" =~ -dev$ || "$VERSION" =~ -beta$ || "$VERSION" =~ -alpha$ ]]; then
    IS_DEV=true
fi

PUBLISH_GITHUB=false
PUBLISH_DOCKER=false
TESTING=false
EXPLICIT_TARGET=false
AUTO_CONFIRM=false

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --all            Publish both GitHub Release and Docker Hub"
    echo "  --github         Publish Debian packages to GitHub Release only"
    echo "  --docker         Build multi-arch Docker image and push to Docker Hub only"
    echo "  --testing, -t    Build multi-arch Docker image tagged as 'testing' and push to Docker Hub"
    echo "  -y, --yes        Auto-confirm publishing without asking"
    echo "  -h, --help       Show this help message"
    echo ""
    exit 0
}

if [ $# -eq 0 ]; then
    usage
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --all)
            PUBLISH_GITHUB=true
            PUBLISH_DOCKER=true
            EXPLICIT_TARGET=true
            shift
            ;;
        --github)
            PUBLISH_GITHUB=true
            EXPLICIT_TARGET=true
            shift
            ;;
        --docker)
            PUBLISH_DOCKER=true
            EXPLICIT_TARGET=true
            shift
            ;;
        --testing|-t)
            PUBLISH_DOCKER=true
            TESTING=true
            EXPLICIT_TARGET=true
            shift
            ;;
        -y|--yes)
            AUTO_CONFIRM=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
    esac
done

if [ "$EXPLICIT_TARGET" = false ]; then
    PUBLISH_GITHUB=true
    PUBLISH_DOCKER=true
fi

if [ "$IS_DEV" = true ]; then
    if [ "$PUBLISH_GITHUB" = true ]; then
        echo -e "${RED}Error: Current version ($VERSION) is a development version.${NC}"
        echo -e "${YELLOW}GitHub releases cannot be published for development versions.${NC}"
        echo -e "${YELLOW}Please finalize the release by running ./release.sh before publishing GitHub releases.${NC}"
        echo -e "${YELLOW}To publish a development Docker image with label 'testing', run:${NC}"
        echo -e "  $0 --docker   (or: $0 --testing)"
        exit 1
    fi
    # Dev versions published to Docker Hub are always tagged as "testing" (no version number)
    TESTING=true
fi

if [ "$TESTING" = true ]; then
    echo -e "${GREEN}=== MonsterMQ Edge Publish Pipeline (Target: rocworks/monstermq-edge:testing) ===${NC}"
    echo "Selected publish targets:"
    echo -e "  • ${BLUE}Docker Hub Multi-Arch Image${NC} -> rocworks/monstermq-edge:testing"
    echo ""

    if [ "$AUTO_CONFIRM" = false ]; then
        read -p "Are you sure you want to publish rocworks/monstermq-edge:testing to Docker Hub? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo -e "${YELLOW}Publish cancelled by user.${NC}"
            exit 0
        fi
    fi
else
    echo -e "${GREEN}=== MonsterMQ Edge Publish Pipeline (Target: ${TAG}) ===${NC}"
    echo "Selected publish targets:"
    [ "$PUBLISH_GITHUB" = true ] && echo -e "  • ${BLUE}Debian Packages${NC} -> GitHub Release"
    [ "$PUBLISH_DOCKER" = true ] && echo -e "  • ${BLUE}Docker Hub Multi-Arch Images${NC}"
    echo ""

    if [ "$AUTO_CONFIRM" = false ]; then
        read -p "Are you sure you want to publish ${TAG} to GitHub/Docker Hub? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo -e "${YELLOW}Publish cancelled by user.${NC}"
            exit 0
        fi
    fi
fi

# 1. Publish Debian Packages to GitHub Releases
if [ "$PUBLISH_GITHUB" = true ]; then
    echo -e "${GREEN}[1/2] Publishing Debian Packages to GitHub Release ${TAG}...${NC}"

    if ! command -v gh &> /dev/null; then
        echo -e "${RED}Error: GitHub CLI ('gh') is not installed.${NC}"
        exit 1
    fi

    if ! gh auth status &> /dev/null; then
        echo -e "${RED}Error: GitHub CLI is not authenticated. Run 'gh auth login'.${NC}"
        exit 1
    fi

    # Verify Debian packages exist
    DEB_ARM64="bin/monstermq-edge_${VERSION}_arm64.deb"
    DEB_ARMHF="bin/monstermq-edge_${VERSION}_armhf.deb"
    DEB_AMD64="bin/monstermq-edge_${VERSION}_amd64.deb"

    if [ ! -f "$DEB_ARM64" ] || [ ! -f "$DEB_ARMHF" ] || [ ! -f "$DEB_AMD64" ]; then
        echo -e "${RED}Error: Debian packages missing in bin/:${NC}"
        [ ! -f "$DEB_ARM64" ] && echo -e "  - missing: $DEB_ARM64"
        [ ! -f "$DEB_ARMHF" ] && echo -e "  - missing: $DEB_ARMHF"
        [ ! -f "$DEB_AMD64" ] && echo -e "  - missing: $DEB_AMD64"
        echo -e "${YELLOW}Please build Debian packages first with: ./build.sh --deb (or ./build.sh --all)${NC}"
        exit 1
    fi

    # Verify tag on remote
    if ! git ls-remote --tags origin "$TAG" | grep -q "$TAG"; then
        echo -e "${YELLOW}Warning: Tag ${TAG} is not on remote. Pushing tag now...${NC}"
        CURRENT_BRANCH=$(git branch --show-current)
        git push origin "$CURRENT_BRANCH"
        git push origin "$TAG"
    fi

    if gh release view "$TAG" &> /dev/null; then
        echo -e "${YELLOW}Uploading Debian packages to existing GitHub release ${TAG}...${NC}"
        gh release upload "$TAG" "$DEB_ARM64" "$DEB_ARMHF" "$DEB_AMD64" --clobber
    else
        echo -e "${YELLOW}Creating GitHub release ${TAG}...${NC}"
        RELEASE_NOTES="releases/v${VERSION}.txt"
        if [ -f "$RELEASE_NOTES" ]; then
            gh release create "$TAG" "$DEB_ARM64" "$DEB_ARMHF" "$DEB_AMD64" --title "Release ${VERSION}" --notes-file "$RELEASE_NOTES"
        else
            gh release create "$TAG" "$DEB_ARM64" "$DEB_ARMHF" "$DEB_AMD64" --title "Release ${VERSION}" --generate-notes
        fi
    fi
    echo -e "${GREEN}✓ Debian packages published to GitHub Release ${TAG}!${NC}"
fi

# 2. Push Multi-Arch Docker Image to Docker Hub
if [ "$PUBLISH_DOCKER" = true ]; then
    if [ "$TESTING" = true ]; then
        echo -e "${GREEN}[2/2] Publishing Multi-Arch Docker Image (testing) to Docker Hub...${NC}"
        (cd docker && ./build.sh -y --testing)
        echo -e "${GREEN}✓ Docker Hub testing image published successfully!${NC}"
    else
        echo -e "${GREEN}[2/2] Publishing Multi-Arch Docker Images to Docker Hub...${NC}"
        (cd docker && ./build.sh -y)
        echo -e "${GREEN}✓ Docker Hub images published successfully!${NC}"
    fi
fi

echo -e "${GREEN}=== Publish Complete ===${NC}"

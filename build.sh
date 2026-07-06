#!/bin/bash

# make.sh - Build wrapper for MonsterMQ Edge
#
# Usage:
#   ./make.sh       - Builds the native binary for the current machine (default)
#   ./make.sh --deb - Builds Debian packages for all target architectures (arm64, armhf, amd64)

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  (no options)     Build the native binary for the current machine (default)"
    echo "  --deb, -deb      Build Debian packages for all target architectures (arm64, armhf, amd64)"
    echo "  -h, --help       Show this help message"
    echo ""
}

BUILD_DEB=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --deb|-deb)
            BUILD_DEB=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            exit 1
            ;;
    esac
done

if [ "$BUILD_DEB" = true ]; then
    echo -e "${GREEN}Building Debian packages for all target architectures...${NC}"
    make deb-all
else
    echo -e "${GREEN}Building native binary for the current machine...${NC}"
    make build
    echo -e "${GREEN}✓ Native binary built at: ${YELLOW}bin/monstermq-edge${NC}"
fi

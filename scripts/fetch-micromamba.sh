#!/bin/sh

# Install micromamba in $PWD/conda/envs/base/bin/micromamba.

set -euo pipefail

# Check if already installed
if [ -f micromamba ]; then
    /bin/echo "micromamba already installed"
    exit 0
fi

arch=$(/usr/bin/uname -sm)

case "$arch" in
    "Linux x86_64")
        URL=https://micro.mamba.pm/api/micromamba/linux-64/latest
        MESSAGE="Running on Linux x86_64"
        ;;
    "Linux arm64")
        URL=https://micro.mamba.pm/api/micromamba/linux-aarch64/latest
        MESSAGE="Running on Linux ARM64"
        ;;
    "Linux ppc64le")
        URL=https://micro.mamba.pm/api/micromamba/linux-ppc64le/latest
        MESSAGE="Running on Linux Power (ppc64le)"
        ;;
    "Darwin x86_64")
        URL=https://micro.mamba.pm/api/micromamba/osx-64/latest
        MESSAGE="Running on macOS Intel (Darwin x86_64)"
        ;;
    "Darwin arm64")
        URL=https://micro.mamba.pm/api/micromamba/osx-arm64/latest
        MESSAGE="Running on macOS Apple Silicon (Darwin arm64)"
        ;;
    *)
        /bin/echo "Platform '$arch' not implemented"
        exit 1
        ;;
esac

/bin/echo "$MESSAGE"
/bin/echo "Fetching $URL"
/usr/bin/curl -L $URL | /usr/bin/tar -xvj bin/micromamba
/bin/mv bin/micromamba .
/bin/rmdir bin || :

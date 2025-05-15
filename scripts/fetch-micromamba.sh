#!/bin/sh

arch=$(/usr/bin/uname -sm)

case "$arch" in
    "Linux x86_64")
        # Linux Intel (x86_64):
        curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest \
            | tar -xvj bin/micromamba
        echo "Running on Linux x86_64"
        ;;
    "Linux arm64")
        # Linux ARM64:
        curl -Ls https://micro.mamba.pm/api/micromamba/linux-aarch64/latest \
            | tar -xvj bin/micromamba
        echo "Running on Linux ARM64"
        ;;
    "Linux ppc64le")
        # Linux Power:
        curl -Ls https://micro.mamba.pm/api/micromamba/linux-ppc64le/latest \
            | tar -xvj bin/micromamba
        echo "Running on Linux Power (ppc64le)"
        ;;
    "Darwin x86_64")
        # macOS Intel (x86_64):
        curl -Ls https://micro.mamba.pm/api/micromamba/osx-64/latest \
            | tar -xvj bin/micromamba
        echo "Running on macOS Intel (Darwin x86_64)"
        ;;
    "Darwin arm64")
        # macOS Silicon/M1 (ARM64):
        curl -Ls https://micro.mamba.pm/api/micromamba/osx-arm64/latest \
            | tar -xvj bin/micromamba
        echo "Running on macOS Apple Silicon (Darwin arm64)"
        ;;
    *)
        echo "Platform '$arch' not implemented"
        ;;
esac

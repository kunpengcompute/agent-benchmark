#!/bin/bash
# Download the final OSWorld image for the host architecture.
set -euo pipefail

if (($#)); then
    echo 'Usage: bash artifacts/images/osworld/download-images.sh (architecture is detected automatically)' >&2
    exit 2
fi

machine_arch="$(uname -m)"
case "$machine_arch" in
    aarch64|arm64)
        image_arch=arm64
        ;;
    x86_64|amd64)
        image_arch=x86
        ;;
    *)
        echo "Unsupported machine architecture: $machine_arch" >&2
        exit 1
        ;;
esac

echo "Pulling OSWorld image for $image_arch ($machine_arch)"
case "$image_arch" in
    arm64)
        docker pull --platform linux/arm64 swr.cn-north-4.myhuaweicloud.com/kunpeng-ai/osworld-official-arm64:latest
        ;;
    x86)
        docker pull --platform linux/amd64 swr.cn-north-4.myhuaweicloud.com/kunpeng-ai/osworld-official-amd64:latest
        ;;
esac

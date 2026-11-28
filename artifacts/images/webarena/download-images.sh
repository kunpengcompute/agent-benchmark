#!/bin/bash
# Download the pre-built webArena images from Huawei Cloud SWR public registry.
set -euo pipefail
REGISTRY="swr.cn-north-4.myhuaweicloud.com/kunpeng-ai"

echo "== Pulling webArena images from SWR =="
docker pull ${REGISTRY}/ubuntu-openclaw-chromium:webarena-arm64
docker pull ${REGISTRY}/webarena-gitlab:latest
docker pull ${REGISTRY}/webarena-verified-shopping:latest

echo "== Verifying browser image =="
docker run --rm ${REGISTRY}/ubuntu-openclaw-chromium:webarena-arm64 /usr/local/node24/bin/agent-browser --version
docker run --rm ${REGISTRY}/ubuntu-openclaw-chromium:webarena-arm64 bash -c 'test -n "$AGENT_BROWSER_PROFILE" && echo "AGENT_BROWSER_PROFILE=$AGENT_BROWSER_PROFILE"'

echo "== Done. =="
echo "  Browser template: aenv pull ${REGISTRY}/ubuntu-openclaw-chromium:webarena-arm64 --name browser-webarena --cpu 2 --memory 4096"
echo "  GitLab: docker run -d -p 8023:8023 -e SITE_HOST=<IP> ${REGISTRY}/webarena-gitlab:latest"
echo "  Magento: docker run -d -p 7770:80 -e SITE_HOST=<IP> ${REGISTRY}/webarena-verified-shopping:latest"

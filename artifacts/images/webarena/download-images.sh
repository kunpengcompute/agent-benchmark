#!/bin/bash
# Download the pre-built webArena images from Huawei Cloud SWR public registry.
set -euo pipefail
REGISTRY="swr.cn-north-4.myhuaweicloud.com/kunpeng-ai"

# The browser image is dual-arch (arm64 / x86_64); pick the host arch.
case "$(uname -m)" in
  x86_64)  BROWSER_TAG="x86_64" ;;
  aarch64) BROWSER_TAG="arm64" ;;
  *) echo "error: unsupported host arch: $(uname -m) (supported: x86_64, aarch64)" >&2; exit 1 ;;
esac
BROWSER="${REGISTRY}/webarena-browser:${BROWSER_TAG}"

echo "== Pulling webArena browser image (${BROWSER_TAG}) =="
docker pull "$BROWSER"

echo "== Verifying browser image =="
docker run --rm "$BROWSER" /usr/local/node24/bin/agent-browser --version
docker run --rm "$BROWSER" bash -c 'if [ -n "$AGENT_BROWSER_PROFILE" ]; then echo "AGENT_BROWSER_PROFILE=$AGENT_BROWSER_PROFILE"; else echo "note: AGENT_BROWSER_PROFILE not set (agent-browser uses its default profile dir)"; fi'

# Site images are arm-only. On an x86 host the site pair deploys on an arm
# machine (the usual two-machine split); pull it here only on arm.
if [ "$BROWSER_TAG" = "arm64" ]; then
  echo "== Pulling webArena site images (arm-only) =="
  docker pull ${REGISTRY}/webarena-gitlab:latest
  docker pull ${REGISTRY}/webarena-verified-shopping:latest
else
  echo "== Skipping site images: webarena-gitlab / webarena-verified-shopping are arm-only =="
  echo "   Deploy the site pair on an arm host and point REPLAY_TARGET_HOST at it."
fi

echo "== Done. =="
echo "  Browser template: aenv pull ${BROWSER} --name browser-webarena --cpu 2 --memory 4096"
if [ "$BROWSER_TAG" = "arm64" ]; then
  echo "  GitLab: docker run -d -p 8023:8023 -e SITE_HOST=<IP> ${REGISTRY}/webarena-gitlab:latest"
  echo "  Magento: docker run -d -p 7770:80 -e SITE_HOST=<IP> ${REGISTRY}/webarena-verified-shopping:latest"
fi

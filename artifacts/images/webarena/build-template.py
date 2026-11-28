#!/usr/bin/env python3
"""Build an AENV template from the pre-built webArena browser image (public registry)."""
import argparse
import subprocess
import sys

def main():
    parser = argparse.ArgumentParser(description="Build AENV template from webArena browser image")
    parser.add_argument("--image", default="swr.cn-north-4.myhuaweicloud.com/kunpeng-ai/webarena-browser:arm64",
                        help="Docker image reference (default: SWR public registry)")
    parser.add_argument("--name", default="browser-webarena", help="AENV template name")
    parser.add_argument("--cpu", type=int, default=2, help="CPU cores")
    parser.add_argument("--memory", type=int, default=4096, help="Memory in MiB")
    parser.add_argument("--timeout", type=int, default=1800, help="Build timeout seconds")
    args = parser.parse_args()

    print(f"== Building AENV template '{args.name}' from: {args.image} ==")
    cmd = ["aenv", "pull", args.image, "--name", args.name, "--cpu", str(args.cpu),
           "--memory", str(args.memory), "--timeout", str(args.timeout)]
    print(f"   Command: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"\nERROR: aenv pull failed (exit {result.returncode})")
        sys.exit(1)
    print(f"\n== Template '{args.name}' is ready. ==")

if __name__ == "__main__":
    main()

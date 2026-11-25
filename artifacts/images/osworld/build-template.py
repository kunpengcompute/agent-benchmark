#!/usr/bin/env python3
"""Build an AENV template from the downloaded OSWorld image."""

import argparse
import json
import platform
import re
import shlex
import shutil
import subprocess
import sys

ARCHES = {"arm64": "arm64 linux", "x86": "amd64 linux"}


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def detect_arch():
    machine = platform.machine().lower()
    if machine in {"aarch64", "arm64"}:
        return "arm64"
    if machine in {"x86_64", "amd64"}:
        return "x86"
    raise ValueError(f"Unknown host architecture {machine!r}; specify --arch")


def command(args, capture=False):
    print(f"$ {shlex.join(args)}", flush=True)
    result = subprocess.run(args, check=True, text=True,
                            stdout=subprocess.PIPE if capture else None,
                            timeout=60 if capture else None)
    return result.stdout.strip() if capture else None


def existing_templates():
    records = json.loads(command(
        ["aenv", "template", "list", "--output", "json"], capture=True))
    if not isinstance(records, list):
        raise ValueError("Unexpected AENV template list format (expected JSON array)")
    states = {}
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("Unexpected AENV template record")
        names = record.get("names")
        status = record.get("buildStatus")
        if (not isinstance(names, list) or
                not all(isinstance(name, str) for name in names) or
                not isinstance(status, str)):
            raise ValueError("AENV template records require names and buildStatus")
        for name in names:
            if name in states:
                raise ValueError(f"Ambiguous existing template name: {name}")
            states[name] = status
    return states


def build(case, arch, cpu, memory, timeout, dry_run):
    commands = [
        ["docker", "image", "inspect", "--format",
         "{{.Architecture}} {{.Os}}", case["remote"]],
        ["docker", "tag", case["remote"], case["target"]],
        ["docker", "push", case["target"]],
        ["aenv", "pull", case["target"], "--name", case["name"],
         "--cpu", str(cpu), "--memory", str(memory), "--timeout", str(timeout)],
    ]
    for index, args in enumerate(commands):
        if dry_run:
            print(f"$ {shlex.join(args)}")
        elif index == 0:
            try:
                actual = command(args, capture=True)
            except subprocess.CalledProcessError as error:
                raise ValueError(
                    f"Cannot inspect local image {case['remote']}; check Docker "
                    "and run bash artifacts/images/osworld/download-images.sh first "
                    "if the image is missing") from error
            if actual != ARCHES[arch]:
                raise ValueError(f"{case['remote']}: expected {ARCHES[arch]}, got {actual}")
        else:
            command(args)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arch", choices=(*ARCHES, "amd64"),
                        help="Target AENV architecture; defaults to this host (amd64 is an alias for x86)")
    parser.add_argument("--registry", default="127.0.0.1:5000",
                        help="Writable registry/repository prefix reachable by AENV")
    parser.add_argument("--name", help="Template name (default: osworld-<arm64|amd64>-offline-desktop)")
    parser.add_argument("--cpu", type=positive_int, default=2,
                        help="Template CPU count (default: 2)")
    parser.add_argument("--memory", type=positive_int, default=4096,
                        help="Template memory in MiB (default: 4096)")
    parser.add_argument("--timeout", type=positive_int, default=1800,
                        help="AENV build wait timeout in seconds (default: 1800)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--list", action="store_true",
                      help="Print the image/template mapping without invoking Docker or AENV")
    mode.add_argument("--dry-run", action="store_true",
                      help="Print commands without invoking Docker or AENV")
    args = parser.parse_args()
    try:
        arch = args.arch or detect_arch()
        if arch == "amd64":
            arch = "x86"
        image_arch = "amd64" if arch == "x86" else "arm64"
        name = args.name or f"osworld-{image_arch}-offline-desktop"
        if not re.fullmatch(r"[A-Za-z0-9_-]+", name):
            raise ValueError("--name must contain only letters, digits, underscores and hyphens")
        registry = args.registry.rstrip("/")
        if (not registry or registry.startswith(("-", "/")) or
                "://" in registry or any(c.isspace() for c in registry)):
            raise ValueError("--registry must be an image prefix without a URL scheme")
        basename = f"osworld-official-{image_arch}:latest"
        case = {"name": name,
                "remote": f"swr.cn-north-4.myhuaweicloud.com/kunpeng-ai/{basename}",
                "target": f"{registry}/{basename}"}
        print(f"Architecture: {arch}; selected templates: 1", flush=True)
        if args.list:
            print(f"{case['name']} -> {case['remote']}")
            return 0
        states = {}
        if not args.dry_run:
            for executable in ("docker", "aenv"):
                if shutil.which(executable) is None:
                    raise ValueError(f"Required command not found: {executable}")
            states = existing_templates()
            status = states.get(name)
            if status is not None and status != "ready":
                raise ValueError(
                    f"{name} already exists with status {status!r}; "
                    "resolve it in AENV before retrying")
        print(f"[1/1] {name}", flush=True)
        if states.get(name) == "ready":
            print("  Reusing existing ready template "
                  "(image and architecture not rechecked; use --name to build a new one).")
            print("Built: 0; reused: 1")
            return 0
        build(case, arch, args.cpu, args.memory, args.timeout, args.dry_run)
        label = "Planned" if args.dry_run else "Built"
        print(f"{label}: 1; reused: 0")
        return 0
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Interrupted; check AENV build status before retrying.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())

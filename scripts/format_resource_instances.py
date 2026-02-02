#!/usr/bin/env python3
import argparse
import gzip
import json
import os
import sys
from pathlib import Path
from typing import Iterable, Optional, Tuple


def load_json_bytes(path: Path) -> Tuple[object, bool]:
    raw = path.read_bytes()
    is_gz = raw[:2] == b"\x1f\x8b"
    if is_gz:
        raw = gzip.decompress(raw)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("utf-8-sig")
    return json.loads(text), is_gz


def dump_json_bytes(obj: object, gzip_output: bool) -> bytes:
    formatted = json.dumps(obj, indent=2, ensure_ascii=True) + "\n"
    data = formatted.encode("utf-8")
    if gzip_output:
        data = gzip.compress(data, mtime=0)
    return data


def iter_instance_files(systems_root: Path, system_filter: Optional[str]) -> Iterable[Path]:
    for system_dir in systems_root.iterdir():
        if not system_dir.is_dir():
            continue
        if system_filter and system_dir.name != system_filter:
            continue
        instances_dir = system_dir / "resource_instances"
        if not instances_dir.is_dir():
            continue
        for root, _dirs, files in os.walk(instances_dir):
            for filename in files:
                if filename.startswith("."):
                    continue
                if not (filename.endswith(".json") or filename.endswith(".rpg")):
                    continue
                yield Path(root) / filename


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pretty-format all resource instance JSON files under systems/*/resource_instances.",
    )
    parser.add_argument(
        "--system",
        default=None,
        help="Limit formatting to a single system folder under systems/ (optional)",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    systems_root = repo_root / "systems"
    if not systems_root.is_dir():
        print(f"Missing systems folder: {systems_root}", file=sys.stderr)
        return 2

    files = list(iter_instance_files(systems_root, args.system))
    if not files:
        target = args.system or "<all>"
        print(f"No resource instance files found for {target}.", file=sys.stderr)
        return 1

    errors = []
    changed = 0
    for path in files:
        try:
            obj, is_gz = load_json_bytes(path)
        except Exception as exc:  # noqa: BLE001 - report parse issues
            errors.append(f"{path}: Failed to parse JSON: {exc}")
            continue
        output = dump_json_bytes(obj, is_gz)
        if path.read_bytes() != output:
            path.write_bytes(output)
            changed += 1

    if errors:
        print("Formatting errors:", file=sys.stderr)
        for err in errors:
            print(f"- {err}", file=sys.stderr)
        print(f"{len(errors)} error(s).", file=sys.stderr)
        return 1

    print(f"Formatted {changed} file(s) out of {len(files)}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

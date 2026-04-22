#!/usr/bin/env python3
"""Compile baseline vs current systems and compare generated JSON with normalization.

Normalization removes known noisy keys:
- macros
- updated_at
"""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


DEFAULT_BUNDLED_TOOLS_ROOT = Path(
    "/Users/blastervla/Repositories/rpg-script-code-extension/bundled_tools"
)
DEFAULT_SYSTEMS = ["5e", "5e2024", "pf2e"]


def run(cmd: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=cwd, text=True, capture_output=True)


def detect_bundled_platform() -> str:
    sys_name = platform.system().lower()
    machine = platform.machine().lower()
    if sys_name == "darwin":
        return "darwin-arm64" if "arm" in machine or "aarch" in machine else "darwin-x64"
    if sys_name == "linux":
        return "linux-x64"
    if sys_name == "windows":
        return "win32-x64"
    raise RuntimeError(f"Unsupported platform: {platform.system()} {platform.machine()}")


def normalize_json(value: Any, ignored_keys: set[str]) -> Any:
    if isinstance(value, dict):
        return {k: normalize_json(v, ignored_keys) for k, v in value.items() if k not in ignored_keys}
    if isinstance(value, list):
        return [normalize_json(v, ignored_keys) for v in value]
    return value


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def compare_json_files(
    baseline_path: Path,
    current_path: Path,
    ignored_keys: set[str],
) -> tuple[bool, str]:
    baseline = load_json(baseline_path)
    current = load_json(current_path)
    baseline_norm = normalize_json(baseline, ignored_keys)
    current_norm = normalize_json(current, ignored_keys)
    if baseline_norm == current_norm:
        return True, ""

    # concise failure reason
    return False, "normalized JSON differs"


def collect_resource_ids(resources_json_path: Path) -> list[str | None]:
    data = load_json(resources_json_path)
    resources = data.get("resources", [])
    return [r.get("id") if isinstance(r, dict) else None for r in resources]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run normalized compiler parity checks.")
    parser.add_argument("--repo", type=Path, default=Path.cwd(), help="Repo root or any path inside repo.")
    parser.add_argument("--baseline-ref", default="HEAD~1", help="Git ref/commit for baseline comparison.")
    parser.add_argument(
        "--bundled-tools-root",
        type=Path,
        default=DEFAULT_BUNDLED_TOOLS_ROOT,
        help="Path to bundled_tools directory.",
    )
    parser.add_argument(
        "--bundled-platform",
        default=detect_bundled_platform(),
        help="Bundled tools platform folder (e.g. darwin-arm64).",
    )
    parser.add_argument(
        "--systems",
        nargs="*",
        default=DEFAULT_SYSTEMS,
        help="Systems to verify (default: 5e 5e2024 pf2e).",
    )
    parser.add_argument(
        "--ignore-key",
        action="append",
        default=["macros", "updated_at"],
        help="JSON keys to ignore during normalized diff (repeatable).",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep temporary worktree/output directories for debugging.",
    )

    args = parser.parse_args()

    repo = args.repo
    git_root_proc = run(["git", "-C", str(repo), "rev-parse", "--show-toplevel"])
    if git_root_proc.returncode != 0:
        print(git_root_proc.stderr, file=sys.stderr)
        return 2
    git_root = Path(git_root_proc.stdout.strip())

    refresh_bin = args.bundled_tools_root / args.bundled_platform / (
        "refresh_system_builder.exe" if args.bundled_platform.startswith("win32") else "refresh_system_builder"
    )
    if not refresh_bin.exists():
        print(f"refresh_system_builder not found at {refresh_bin}", file=sys.stderr)
        return 2

    temp_root = Path(tempfile.mkdtemp(prefix="rpg_parity_"))
    baseline_worktree = temp_root / "baseline_worktree"
    baseline_output = temp_root / "baseline_output"
    current_output = temp_root / "current_output"

    try:
        add_worktree = run(
            ["git", "-C", str(git_root), "worktree", "add", "--detach", str(baseline_worktree), args.baseline_ref]
        )
        if add_worktree.returncode != 0:
            print(add_worktree.stdout)
            print(add_worktree.stderr, file=sys.stderr)
            return 2

        baseline_cmd = [
            str(refresh_bin),
            f"--base={baseline_worktree / 'systems'}",
            f"--output={baseline_output}",
            "--clean",
            "--structured-diagnostics",
        ]
        current_cmd = [
            str(refresh_bin),
            f"--base={git_root / 'systems'}",
            f"--output={current_output}",
            "--clean",
            "--structured-diagnostics",
        ]

        baseline_run = run(baseline_cmd, cwd=baseline_worktree)
        if baseline_run.returncode != 0:
            print(baseline_run.stdout)
            print(baseline_run.stderr, file=sys.stderr)
            return 2

        current_run = run(current_cmd, cwd=git_root)
        if current_run.returncode != 0:
            print(current_run.stdout)
            print(current_run.stderr, file=sys.stderr)
            return 2

        ignored = set(args.ignore_key)

        checks: list[tuple[str, Path, Path]] = []
        checks.append(
            (
                "root/systems.json",
                baseline_output / "systems.json",
                current_output / "systems.json",
            )
        )

        for system in args.systems:
            checks.append(
                (
                    f"{system}/system.composed.json",
                    baseline_output / system / "system.composed.json",
                    current_output / system / "system.composed.json",
                )
            )
            checks.append(
                (
                    f"{system}/resources.json",
                    baseline_output / system / "resources.json",
                    current_output / system / "resources.json",
                )
            )

        failures: list[str] = []

        for label, bpath, cpath in checks:
            if not bpath.exists() or not cpath.exists():
                failures.append(f"{label}: missing file(s)")
                continue
            ok, reason = compare_json_files(bpath, cpath, ignored)
            if ok:
                print(f"PASS {label}")
            else:
                failures.append(f"{label}: {reason}")

        for system in args.systems:
            bres = baseline_output / system / "resources.json"
            cres = current_output / system / "resources.json"
            if bres.exists() and cres.exists():
                b_ids = collect_resource_ids(bres)
                c_ids = collect_resource_ids(cres)
                if b_ids != c_ids:
                    failures.append(f"{system}/resources.json: resource id ordering/content differs")
                else:
                    print(f"PASS {system} resource ids")

        if failures:
            print("\nFAILURES:")
            for failure in failures:
                print(f"- {failure}")
            if args.keep_temp:
                print(f"\nKept temp directory: {temp_root}")
            return 1

        print("\nParity check passed (after key normalization).")
        if args.keep_temp:
            print(f"Kept temp directory: {temp_root}")
        return 0
    finally:
        remove_worktree = run(["git", "-C", str(git_root), "worktree", "remove", "--force", str(baseline_worktree)])
        if remove_worktree.returncode != 0:
            # best effort cleanup; keep temp for inspection if this fails
            args.keep_temp = True
        if not args.keep_temp:
            shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())

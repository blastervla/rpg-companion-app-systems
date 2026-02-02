#!/usr/bin/env python3
import argparse
import gzip
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


BASE_STAT_PATTERN = re.compile(r"^base\s+([^\s]+)\s+([A-Za-z0-9_]+)\s*\(")
META_STATS = {"id", "updated_at"}


@dataclass(frozen=True)
class StatType:
    kind: str
    is_array: bool
    resource_type: Optional[str] = None

    def element_type(self) -> "StatType":
        return StatType(self.kind, False, self.resource_type)


def parse_type(type_token: str) -> StatType:
    is_array = type_token.endswith("[]")
    core = type_token[:-2] if is_array else type_token
    if core.startswith("resource<") and core.endswith(">"):
        return StatType("resource", is_array, core[len("resource<") : -1])
    if core == "resource":
        return StatType("resource", is_array, None)
    if core in {"string", "bool", "integer", "photo"}:
        return StatType(core, is_array, None)
    return StatType("unknown", is_array, None)


def load_schema(resources_root: Path) -> Dict[str, Dict[str, StatType]]:
    schema: Dict[str, Dict[str, StatType]] = {}
    for root, _dirs, files in os.walk(resources_root):
        if "stats.rpgs" not in files:
            continue
        resource_id = Path(root).name
        stats_path = Path(root) / "stats.rpgs"
        schema[resource_id] = parse_stats_file(stats_path)
    return schema


def parse_stats_file(path: Path) -> Dict[str, StatType]:
    stats: Dict[str, StatType] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped.startswith("base "):
            continue
        match = BASE_STAT_PATTERN.match(stripped)
        if not match:
            continue
        type_token, stat_name = match.groups()
        stats[stat_name] = parse_type(type_token)
    return stats


def read_json(path: Path) -> Any:
    raw = path.read_bytes()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    try:
        return json.loads(raw.decode("utf-8"))
    except UnicodeDecodeError:
        return json.loads(raw.decode("utf-8-sig"))


def format_path(path_stack: List[str]) -> str:
    return " -> ".join(path_stack) if path_stack else "<root>"


def display_path(path: Path, repo_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except ValueError:
        return str(path)


def resource_label(resource_obj: Dict[str, Any]) -> str:
    rid = resource_obj.get("resource_id")
    stats = resource_obj.get("stats")
    rid_text = f"resource_id='{rid}'" if isinstance(rid, str) else "resource_id<?>"
    if isinstance(stats, dict):
        stat_id = stats.get("id")
        if isinstance(stat_id, str) and stat_id:
            return f"{rid_text} (id='{stat_id}')"
    return rid_text


def add_error(
    errors: List[str],
    file_path: Path,
    repo_root: Path,
    path_stack: List[str],
    message: str,
) -> None:
    location = format_path(path_stack)
    errors.append(f"{display_path(file_path, repo_root)}: {location}: {message}")


def validate_resource_instance(
    resource_obj: Any,
    schema: Dict[str, Dict[str, StatType]],
    errors: List[str],
    file_path: Path,
    repo_root: Path,
    path_stack: List[str],
) -> None:
    if not isinstance(resource_obj, dict):
        add_error(
            errors,
            file_path,
            repo_root,
            path_stack,
            f"Expected object for resource, got {type(resource_obj).__name__}",
        )
        return

    rid = resource_obj.get("resource_id")
    if not isinstance(rid, str) or not rid:
        add_error(
            errors,
            file_path,
            repo_root,
            path_stack,
            "Missing or invalid resource_id",
        )
        return

    stats_schema = schema.get(rid)
    if stats_schema is None:
        add_error(
            errors,
            file_path,
            repo_root,
            path_stack + [f"resource_id='{rid}'"],
            "Unknown resource_id (no stats.rpgs found)",
        )
        return

    stats = resource_obj.get("stats")
    if not isinstance(stats, dict):
        add_error(
            errors,
            file_path,
            repo_root,
            path_stack + [resource_label(resource_obj)],
            f"Missing or invalid stats object, got {type(stats).__name__}",
        )
        return

    current_stack = path_stack + [resource_label(resource_obj)]
    for stat_name, stat_value in stats.items():
        if stat_name in META_STATS:
            continue
        stat_type = stats_schema.get(stat_name)
        if stat_type is None:
            add_error(
                errors,
                file_path,
                repo_root,
                current_stack + [f"stats.{stat_name}"],
                "Unknown stat for this resource",
            )
            continue
        if not isinstance(stat_value, dict) or "value" not in stat_value:
            add_error(
                errors,
                file_path,
                repo_root,
                current_stack + [f"stats.{stat_name}"],
                "Expected an object with a 'value' field",
            )
            continue
        validate_value(
            stat_value.get("value"),
            stat_type,
            schema,
            errors,
            file_path,
            repo_root,
            current_stack + [f"stats.{stat_name}.value"],
        )


def validate_value(
    value: Any,
    stat_type: StatType,
    schema: Dict[str, Dict[str, StatType]],
    errors: List[str],
    file_path: Path,
    repo_root: Path,
    path_stack: List[str],
) -> None:
    if value is None:
        return

    if stat_type.is_array:
        if not isinstance(value, list):
            add_error(
                errors,
                file_path,
                repo_root,
                path_stack,
                f"Expected array, got {type(value).__name__}",
            )
            return
        element_type = stat_type.element_type()
        for idx, item in enumerate(value):
            if item is None and element_type.kind == "resource":
                continue
            validate_value(
                item,
                element_type,
                schema,
                errors,
                file_path,
                repo_root,
                path_stack + [f"[{idx}]"],
            )
        return

    kind = stat_type.kind
    if kind == "unknown":
        return
    if kind == "string":
        if not isinstance(value, str):
            add_error(
                errors,
                file_path,
                repo_root,
                path_stack,
                f"Expected string, got {type(value).__name__}",
            )
        return
    if kind == "bool":
        if not isinstance(value, bool):
            add_error(
                errors,
                file_path,
                repo_root,
                path_stack,
                f"Expected bool, got {type(value).__name__}",
            )
        return
    if kind == "integer":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            add_error(
                errors,
                file_path,
                repo_root,
                path_stack,
                f"Expected number, got {type(value).__name__}",
            )
        return
    if kind == "photo":
        if not isinstance(value, dict):
            add_error(
                errors,
                file_path,
                repo_root,
                path_stack,
                f"Expected photo object, got {type(value).__name__}",
            )
            return
        if "url" in value and not isinstance(value["url"], str):
            add_error(
                errors,
                file_path,
                repo_root,
                path_stack + ["url"],
                f"Expected url string, got {type(value['url']).__name__}",
            )
        return
    if kind == "resource":
        if not isinstance(value, dict):
            add_error(
                errors,
                file_path,
                repo_root,
                path_stack,
                f"Expected resource object, got {type(value).__name__}",
            )
            return
        expected = stat_type.resource_type
        actual = value.get("resource_id")
        if expected and actual != expected:
            add_error(
                errors,
                file_path,
                repo_root,
                path_stack,
                f"Expected resource_id '{expected}', got '{actual}'",
            )
        validate_resource_instance(
            value,
            schema,
            errors,
            file_path,
            repo_root,
            path_stack,
        )


def iter_instance_files(instances_root: Path) -> List[Path]:
    files: List[Path] = []
    for root, _dirs, filenames in os.walk(instances_root):
        for filename in filenames:
            if filename.startswith("."):
                continue
            if not filename.endswith((".json", ".rpg")):
                continue
            files.append(Path(root) / filename)
    return files


def infer_system_from_path(path: Path, repo_root: Path) -> Optional[str]:
    try:
        rel = path.resolve().relative_to(repo_root.resolve())
    except ValueError:
        return None
    parts = list(rel.parts)
    for idx, part in enumerate(parts):
        if part == "systems" and idx + 1 < len(parts):
            return parts[idx + 1]
    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate resource instance JSON against system stats.rpgs schema.",
    )
    parser.add_argument(
        "--system",
        default=None,
        help="System folder name under systems/ (default: 5e or inferred from --file)",
    )
    parser.add_argument(
        "--instances",
        default=None,
        help="Override resource_instances path (default: systems/<system>/resource_instances)",
    )
    parser.add_argument(
        "--file",
        default=None,
        help="Validate a single resource instance file",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    if args.file:
        file_path = Path(args.file)
        if not file_path.is_file():
            print(f"Missing file: {file_path}", file=sys.stderr)
            return 2
        system_name = args.system or infer_system_from_path(file_path, repo_root) or "5e"
    else:
        system_name = args.system or "5e"

    system_root = repo_root / "systems" / system_name
    resources_root = system_root / "system" / "resources"
    instances_root = Path(args.instances) if args.instances else system_root / "resource_instances"

    if not resources_root.is_dir():
        print(f"Missing resources folder: {resources_root}", file=sys.stderr)
        return 2
    if not args.file and not instances_root.is_dir():
        print(f"Missing resource_instances folder: {instances_root}", file=sys.stderr)
        return 2

    schema = load_schema(resources_root)
    errors: List[str] = []
    instance_files = [file_path] if args.file else iter_instance_files(instances_root)

    for path in instance_files:
        try:
            data = read_json(path)
        except Exception as exc:  # noqa: BLE001 - report parse issues
            add_error(
                errors,
                path,
                repo_root,
                [],
                f"Failed to parse JSON: {exc}",
            )
            continue
        validate_resource_instance(data, schema, errors, path, repo_root, [])

    if errors:
        print("Validation errors:", file=sys.stderr)
        for err in errors:
            print(f"- {err}", file=sys.stderr)
        print(f"{len(errors)} error(s) found in {len(instance_files)} file(s).", file=sys.stderr)
        return 1

    print(f"OK: {len(instance_files)} resource instance file(s) validated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

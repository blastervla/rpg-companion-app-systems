#!/usr/bin/env python3
import gzip
import json
from pathlib import Path
from typing import Any, Tuple


def load_json_bytes(path: Path) -> Tuple[Any, bool, str]:
    raw = path.read_bytes()
    is_gz = raw[:2] == b"\x1f\x8b"
    if is_gz:
        raw = gzip.decompress(raw)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("utf-8-sig")
    return json.loads(text), is_gz, text


def detect_formatting(raw_text: str) -> Tuple[bool, bool]:
    pretty = "\n" in raw_text
    with_spaces = '\": ' in raw_text
    return pretty, with_spaces


def dump_json_bytes(
    obj: Any,
    gzip_output: bool,
    pretty: bool,
    with_spaces: bool,
) -> bytes:
    if pretty:
        formatted = json.dumps(obj, indent=2, ensure_ascii=True) + "\n"
    else:
        separators = (", ", ": ") if with_spaces else (",", ":")
        formatted = json.dumps(obj, ensure_ascii=True, separators=separators) + "\n"
    data = formatted.encode("utf-8")
    if gzip_output:
        data = gzip.compress(data, mtime=0)
    return data


def write_json(path: Path, obj: Any, gzip_output: bool, raw_text: str) -> None:
    pretty, with_spaces = detect_formatting(raw_text)
    path.write_bytes(dump_json_bytes(obj, gzip_output, pretty, with_spaces))

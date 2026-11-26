# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Select pytest files and retain evidence from each standalone test invocation."""

import argparse
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import xml.etree.ElementTree as ET


TEST_ROOT = Path("integration_tests/src/main/python")


@lru_cache(maxsize=1)
def shard_overrides(root):
    catalog = json.loads((root / ".github/gpu-ci-suites.json").read_text())
    overrides = catalog["suites"]["integration"]["shard_overrides"]
    if not isinstance(overrides, dict) or any(type(s) is not int or s not in (1, 2) for s in overrides.values()):
        raise ValueError("Integration shard overrides must map filenames to shard 1 or 2")
    return overrides


def shard_for(filename, root=Path(".")):
    return shard_overrides(root).get(filename, 1 + hashlib.sha256(filename.encode()).digest()[0] % 2)


def targets(shard):
    if "TARGETS_JSON" not in os.environ:
        return []
    nodes = json.loads(os.environ["TARGETS_JSON"])
    if not isinstance(nodes, list) or not nodes:
        raise ValueError("A targeted run requires a nonempty list of pytest node IDs")
    for node in nodes:
        if not isinstance(node, str) or not re.fullmatch(r"[a-z0-9_]+_test\.py::test_[a-zA-Z0-9_]+", node):
            raise ValueError("Invalid targeted pytest node ID")
        filename = node.split("::")[0]
        if not (TEST_ROOT / filename).is_file() or shard_for(filename) != shard:
            raise ValueError(f"Unknown or wrong-shard target: {node}")
    if len(set(nodes)) != len(nodes):
        raise ValueError("Duplicate pytest target")
    return nodes


def select_files(shard):
    nodes = targets(shard)
    if nodes:
        return " ".join(nodes)
    files = {p.relative_to(TEST_ROOT).as_posix() for p in TEST_ROOT.rglob("*.py")
             if p.is_file() and (p.name.startswith("test_") or p.name.endswith("_test.py"))}
    selected = sorted(f for f in files if shard_for(f) == shard)
    if not selected or any(any(c.isspace() for c in item) for item in selected):
        raise ValueError("Empty partition or a filename unsupported by the pytest launcher")
    return " ".join(selected)


def collect(target, output, phase, before):
    previous = set(before.read_text(encoding="utf-8").splitlines())
    reports = sorted(p for p in target.glob("run_dir-*/TEST-pytest-*.xml") if str(p) not in previous)
    if not reports:
        raise ValueError(f"{phase}: pytest produced no JUnit report")
    failed = False
    required = set(targets(int(os.environ["IT_SHARD"]))) if "TARGETS_JSON" in os.environ else set()
    executed, skipped = set(), set()
    for i, path in enumerate(reports, start=1):
        dest = output / f"{phase}-{i}.xml"
        shutil.copyfile(path, dest)
        tests = list(ET.parse(dest).getroot().iter("testcase"))
        ran = sum(t.find("skipped") is None for t in tests)
        if required:
            for test in tests:
                module = test.get("classname", "").rsplit(".", 1)[-1]
                name = test.get("name", "").split("[", 1)[0]
                seen = skipped if test.find("skipped") is not None else executed
                seen.add(f"{module}.py::{name}")
        print(f"{dest.name}: {ran}/{len(tests)} cases executed", flush=True)
        failed |= not tests or (not required and not ran)
        failed |= any(t.find("error") is not None or t.find("failure") is not None for t in tests)
    if failed:
        raise ValueError(f"{phase}: failed or no executed pytest cases")
    missing = required - executed - skipped
    if missing:
        raise ValueError(f"{phase}: missing targets: {', '.join(sorted(missing))}")
    if required - executed:
        print(f"{phase}: collected but skipped: {', '.join(sorted(required - executed))}; running full shard")
        return 42  # Request full coverage, only after a successful launcher and complete JUnit evidence.


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    files = commands.add_parser("files")
    files.add_argument("shard", type=int, choices=(1, 2))
    evidence = commands.add_parser("reports")
    evidence.add_argument("target", type=Path)
    evidence.add_argument("output", type=Path)
    evidence.add_argument("phase")
    evidence.add_argument("before", type=Path)
    args = parser.parse_args()
    if args.command == "files":
        print(select_files(args.shard))
    else:
        raise SystemExit(collect(args.target, args.output, args.phase, args.before))

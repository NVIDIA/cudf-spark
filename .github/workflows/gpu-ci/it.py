# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Select pytest files and retain evidence from each standalone test invocation."""

import argparse
import hashlib
from pathlib import Path
import shutil
import xml.etree.ElementTree as ET


TEST_ROOT = Path("integration_tests/src/main/python")
# Balance the slow files using per-file JUnit durations; other files get stable assignments.
# Sample: Blossom PR builds 15834, 15838, 15839 (mvn verify and CI scala 2.13).
SHARDS = {"array_test.py": 1, "conditionals_test.py": 2, "hash_aggregate_test.py": 2,
          "parquet_test.py": 2}


def select_files(shard):
    files = {p.relative_to(TEST_ROOT).as_posix() for p in TEST_ROOT.rglob("*.py")
             if p.is_file() and (p.name.startswith("test_") or p.name.endswith("_test.py"))}
    selected = sorted(f for f in files if SHARDS.get(
        f, 1 + hashlib.sha256(f.encode()).digest()[0] % 2) == shard)
    if not selected or any(any(c.isspace() for c in item) for item in selected):
        raise ValueError("Empty partition or a filename unsupported by the pytest launcher")
    return " ".join(selected)


def collect(target, output, phase, before):
    previous = set(before.read_text(encoding="utf-8").splitlines())
    reports = sorted(p for p in target.glob("run_dir-*/TEST-pytest-*.xml") if str(p) not in previous)
    if not reports:
        raise ValueError(f"{phase}: pytest produced no JUnit report")
    failed = False
    for i, path in enumerate(reports, start=1):
        dest = output / f"{phase}-{i}.xml"
        shutil.copyfile(path, dest)
        tests = list(ET.parse(dest).getroot().iter("testcase"))
        ran = sum(t.find("skipped") is None for t in tests)
        print(f"{dest.name}: {ran}/{len(tests)} cases executed", flush=True)
        failed |= not ran or any(t.find("error") is not None or t.find("failure") is not None for t in tests)
    if failed:
        raise ValueError(f"{phase}: failed or no executed pytest cases")


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
        collect(args.target, args.output, args.phase, args.before)

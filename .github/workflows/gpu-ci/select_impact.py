# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Select tests for one runtime from the complete base-to-merge diff.

Examples (runtime, changed files -> plan):
  standalone, pom.xml and a test file -> {"mode": "full"}
  databricks, even a single standalone test file -> {"mode": "full"}
See ut_impact.py and it_impact.py for inputs that select individual suites or cases.
"""

import argparse
import json
from pathlib import Path
import re
import subprocess

from it_impact import affected_cases
from ut_impact import affected_suites


def modified_files(root, base, merge):
    """Only ordinary edits are eligible; additions, deletions, renames and mode changes need full CI."""
    if not all(re.fullmatch(r"[0-9a-f]{40}", rev) for rev in (base, merge)):
        return []
    raw = subprocess.check_output(["git", "diff", "--raw", "-z", "--no-renames", "--no-ext-diff",
                                   base, merge, "--"], cwd=root).decode("utf-8")
    fields = raw.rstrip("\0").split("\0")
    if len(fields) % 2 or not all(re.fullmatch(r":100644 100644 [0-9a-f]+ [0-9a-f]+ M", meta)
                                  for meta in fields[::2]):
        return []
    return fields[1::2]


def select_impact(root, base, merge, *, runtime):
    if runtime == "databricks":
        return {"mode": "full"}
    if runtime != "standalone":
        raise ValueError(f"Unknown test runtime: {runtime}")
    try:
        paths = modified_files(root, base, merge)
    except (OSError, UnicodeError, subprocess.CalledProcessError):
        paths = []
    if paths and all(p.endswith("_test.py") for p in paths):
        cases = affected_cases(root, base, merge, paths)
        if cases:
            return {"mode": "integration", "shards": cases}
    elif paths and all(p.endswith("Suite.scala") for p in paths):
        suites = affected_suites(root, base, merge, paths)
        if suites:
            return {"mode": "unit", "suites": suites}
    return {"mode": "full"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runtime", required=True, choices=("standalone", "databricks"))
    parser.add_argument("base", help="Base commit SHA")
    parser.add_argument("merge", help="Tested merge commit SHA")
    args = parser.parse_args()
    print(json.dumps(select_impact(Path.cwd(), args.base, args.merge, runtime=args.runtime)))

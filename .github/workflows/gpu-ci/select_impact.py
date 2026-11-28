# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Select tests for one runtime from the complete base-to-merge diff.

Examples (runtime, changed files -> plan):
  standalone, pom.xml and a test file -> full UT and IT
  standalone, check-reports.py and one isolated pytest case -> full UT and targeted IT
  standalone, jenkins/databricks/test.sh only -> no standalone jobs; Databricks runs
See ut_impact.py and it_impact.py for inputs that select individual suites or cases.
"""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys

from it_impact import affected_cases
from ut_impact import affected_suites


RULES_PATH = ".github/workflows/gpu-ci/impact-rules.json"
DB_RULES_PATH = ".github/workflows/databricks-ci/paths.json"


def base_json(root, base, path):
    raw = subprocess.check_output(["git", "show", f"{base}:{path}"], cwd=root,
                                  stderr=subprocess.DEVNULL)
    return json.loads(raw)


def load_rules(root, base):
    # A PR cannot opt itself into narrower testing by changing its own rules.
    rules = base_json(root, base, RULES_PATH)
    if not isinstance(rules, dict) or rules.keys() != {"file_owners", "scala_suite_bases", "pytest_decorators"}:
        raise ValueError("expected file_owners, scala_suite_bases and pytest_decorators")
    owners = rules["file_owners"]
    if not isinstance(owners, dict) or any(v not in ("unit", "integration") for v in owners.values()):
        raise ValueError("file_owners must map exact paths to unit or integration")
    for key in ("scala_suite_bases", "pytest_decorators"):
        names = rules[key]
        pattern = r"[A-Za-z_]\w*" if key == "scala_suite_bases" else r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*"
        if not isinstance(names, list) or not all(isinstance(n, str) and re.fullmatch(pattern, n, re.ASCII)
                                                  for n in names):
            raise ValueError(f"{key} must be a list of literal names")
    return rules


def db_only_files(root, base, paths):
    # Do not omit standalone coverage if the target branch and the running DB
    # controller disagree about a file's ownership.
    controller = Path(__file__).resolve().parents[1] / "databricks-ci/paths.json"
    current = json.loads(controller.read_text())["cluster"]["exclusive_files"]
    if not isinstance(current, list):
        raise ValueError("invalid Databricks exclusive_files")
    hits = set(current).intersection(paths)
    if hits:
        target = base_json(root, base, DB_RULES_PATH)["cluster"]["exclusive_files"]
        if not isinstance(target, list) or not hits.issubset(target):
            raise ValueError("Databricks-only paths differ on the target branch")
    return hits


def modified_files(root, base, merge):
    """Only ordinary edits are eligible; additions, deletions, renames and mode changes need full CI."""
    if not all(re.fullmatch(r"[0-9a-f]{40}", rev) for rev in (base, merge)):
        return {}
    raw = subprocess.check_output(["git", "diff", "--raw", "-z", "--no-renames", "--no-ext-diff",
                                   base, merge, "--"], cwd=root).decode("utf-8")
    fields = raw.rstrip("\0").split("\0")
    if len(fields) % 2 or not all(re.fullmatch(r":(100644|100755) \1 [0-9a-f]+ [0-9a-f]+ M", meta)
                                  for meta in fields[::2]):
        return {}
    return {path: meta[1:7] for meta, path in zip(fields[::2], fields[1::2])}


def select_impact(root, base, merge, *, runtime):
    full = {"unit": None, "integration": None}
    if runtime == "databricks":
        return full
    if runtime != "standalone":
        raise ValueError(f"Unknown test runtime: {runtime}")
    try:
        paths = modified_files(root, base, merge)
        if not paths:
            return full
        rules = load_rules(root, base)
        exclusive = db_only_files(root, base, paths)
        if exclusive.intersection(rules["file_owners"]):
            raise ValueError("conflicting standalone and Databricks file owners")
        full_kinds = set()
        scala, python = [], []
        for path, mode in paths.items():
            if path in exclusive:
                continue
            if path in rules["file_owners"]:
                full_kinds.add(rules["file_owners"][path])
            elif mode != "100644":
                return full  # Only a known launcher can be an isolated executable edit.
            elif path.endswith("Suite.scala"):
                scala.append(path)
            elif path.endswith("_test.py"):
                python.append(path)
            else:
                return full

        suites = affected_suites(root, base, merge, scala, rules["scala_suite_bases"]) if scala else []
        cases = affected_cases(root, base, merge, python, rules["pytest_decorators"]) if python else {}
        if (scala and not suites) or (python and not cases):
            return full  # Shared helpers, decorators or dependencies could affect either family.
        return {"unit": None if "unit" in full_kinds else suites,
                "integration": None if "integration" in full_kinds else cases}
    except (OSError, ValueError, KeyError, TypeError, RecursionError, subprocess.CalledProcessError) as exc:
        print(f"Cannot determine standalone test impact; running full coverage: {exc}", file=sys.stderr)
    return full


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runtime", required=True, choices=("standalone", "databricks"))
    parser.add_argument("base", help="Base commit SHA")
    parser.add_argument("merge", help="Tested merge commit SHA")
    args = parser.parse_args()
    print(json.dumps(select_impact(Path.cwd(), args.base, args.merge, runtime=args.runtime)))

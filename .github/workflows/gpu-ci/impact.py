# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Select tests for one runtime from the complete base-to-merge diff.

Examples:
  * Only the ORC retry suite changes (#16246): run that class on the configured UT shims.
    If two eligible leaf suites change, run both classes on those same shims.
  * Only the Parquet decimal scale test body changes (#17105): run that pytest
    case in its standalone IT shard on each Scala version.
  * Product code, shared fixtures or mixed changes: keep the full configured workload.
  * The same Parquet edit with --runtime databricks: keep the full DB workload.
    Standalone sharding does not cover DB-specific modes or runtime versions.
"""

import argparse
import ast
import json
from pathlib import Path
import re
import subprocess

from it import TEST_ROOT, shard_for
from unit_impact import affected_suites


def ordinary_decorator(node):
    marker = ast.unparse(node.func if isinstance(node, ast.Call) else node)
    if marker == "pytest.mark.parametrize":
        # Literal parameter sets cannot introduce per-parameter skip or supplemental markers.
        if not isinstance(node, ast.Call) or len(node.args) != 2 or node.keywords:
            return False
        try:
            return isinstance(ast.literal_eval(node.args[1]), (tuple, list))
        except (ValueError, TypeError):
            return False
    return marker in ("ignore_order", "pytest.mark.ignore_order", "pytest.mark.premerge_ci_1")


def split_cases(source):
    """Keep everything except the bodies of ordinary standalone tests byte-for-byte."""
    lines = source.splitlines(keepends=True)
    outside, bodies, end = [], {}, 0
    # A CI slice marker is immaterial when the launcher explicitly requests a case.
    scope = re.sub(r"(?m)^pytestmark = \[pytest\.mark\.premerge_ci_1\]$", "", source)
    if re.search(r"\b(?:pytestmark|pytest_plugins|pytest_collection_modifyitems|pytest_generate_tests)\b", scope):
        return source, {}
    for node in ast.parse(source).body:
        if (not isinstance(node, ast.FunctionDef) or not re.fullmatch(r"test_[a-zA-Z0-9_]+", node.name)
                or not all(ordinary_decorator(d) for d in node.decorator_list)):
            continue
        start = node.body[0].lineno - 1
        if start < node.lineno or not lines[start - 1].rstrip().endswith(":"):
            continue  # Keep inline bodies and ambiguous boundaries out of the optimization.
        if node.name in bodies:
            return source, {}
        outside.extend(lines[end:start])
        end = node.end_lineno
        bodies[node.name] = "".join(lines[start:end])
    outside.extend(lines[end:])
    return "".join(outside), bodies


def referenced(root, rev, path, names):
    stem = Path(path).stem
    words = [stem, *names]
    patterns = [arg for word in words for arg in ("-e", word)]
    # DB jobs have a separate launcher and collection policy, not part of standalone IT.
    found = subprocess.run(["git", "grep", "-I", "-l", "-z", "-F", "-w", *patterns, rev, "--",
                            "*.py", "*.sh", ":(exclude)jenkins/databricks/**", f":(exclude){path}"],
                           capture_output=True, cwd=root, encoding="utf-8")
    if found.returncode == 1:
        return False
    found.check_returncode()
    for blob in found.stdout.rstrip("\0").split("\0"):
        if not blob.endswith(".py"):
            return True
        tree = ast.parse(subprocess.check_output(["git", "show", blob], cwd=root))
        for node in ast.walk(tree):
            if (isinstance(node, ast.ImportFrom) and node.module == stem and
                    all(alias.name not in {"*", *names} for alias in node.names)):
                node.module = None  # Importing a specific unrelated helper doesn't invoke the tests.
        if re.search(r"\b(?:" + "|".join(words) + r")\b", ast.dump(tree)):
            return True
    return False


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


def affected_cases(root, base, merge, paths):
    """Return shard -> pytest node IDs, or {} to keep the full configured workload."""
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=root).decode("utf-8")

    selected = {}
    try:
        for path in paths:
            filename = Path(path).name
            if Path(path).parent != TEST_ROOT or not re.fullmatch(r"[a-z0-9_]+_test\.py", filename):
                return {}
            old, new = (git("show", f"{rev}:{path}") for rev in (base, merge))
            old_module, before = split_cases(old)
            new_module, after = split_cases(new)
            if old_module != new_module or before.keys() != after.keys():
                return {}
            names = sorted(name for name in before if before[name] != after[name])
            if not names:
                return {}
            for name in names:
                if any(len(re.findall(rf"\b{name}\b", src)) != 1 for src in (old, new)):
                    return {}
            if any(referenced(root, rev, path, names) for rev in (base, merge)):
                return {}
            selected.setdefault(shard_for(filename, root), []).extend(f"{filename}::{name}" for name in names)
    except (OSError, subprocess.CalledProcessError, SyntaxError, UnicodeError, ValueError, RecursionError):
        return {}
    return {shard: sorted(nodes) for shard, nodes in selected.items()}


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

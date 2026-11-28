# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Plan Databricks CI from authenticated PR metadata and Git objects.

Examples: README.md -> skip; delta-lake/pom.xml -> run both DBRs and both parts;
README.md + [databricks] -> run; [skip ci] + [databricks] -> skip.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess


def cluster_impact(paths):
    # Read the policy next to this trusted script, not from the PR checkout.
    rules = json.loads(Path(__file__).with_name("paths.json").read_text())["cluster"]
    patterns, excluded, exclusive = rules["include_regex"], rules["skip_if_all_under"], rules["exclusive_files"]
    if not (isinstance(patterns, list) and patterns and isinstance(excluded, list) and isinstance(exclusive, list)
            and all(isinstance(s, str) and s for s in patterns + excluded + exclusive)):
        raise ValueError("Invalid Databricks cluster rules in paths.json")
    match = re.compile("|".join(f"(?:{p})" for p in patterns).encode(), re.DOTALL)
    prefixes = tuple(p.encode() for p in excluded)
    return (bool({p.encode() for p in exclusive}.intersection(paths)) or
            (any(not path.startswith(prefixes) for path in paths) and any(match.search(path) for path in paths)))


def select(repo, base, head, merge, title):
    if not all(re.fullmatch(r"[0-9a-f]{40}", rev) for rev in (base, head, merge)):
        raise ValueError("Expected full PR base, head and merge commit SHAs")
    if not title.strip():
        raise ValueError("Missing authenticated PR title")

    def git(*args):
        return subprocess.check_output(["git", "--no-replace-objects", "-C", repo, *args],
                                       timeout=30)

    # Even a skip must be for the currently authorized head merged into its exact base.
    lineage = git("rev-list", "--no-walk", "--parents", merge, "--").decode("ascii").split()
    if lineage != [merge, base, head]:
        raise ValueError("Test merge parents do not match the authorized PR base and head")

    title = title.lower()
    if "[skip ci]" in title:
        run, reason = False, "skip-ci-title"
    elif "[databricks]" in title:
        run, reason = True, "databricks-title"
    else:
        try:
            diff = git("diff", "--name-only", "-z", "--no-renames", "--no-ext-diff",
                       "--no-textconv", "--ignore-submodules=none", base, merge, "--")
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            run, reason = True, "changed-paths-unavailable"
        else:
            paths = diff.rstrip(b"\0").split(b"\0") if diff else []
            run = cluster_impact(paths)
            reason = "databricks-path" if run else "no-databricks-impact"

    return {"run": run, "reduced_it": "[reduced-it]" in title, "reason": reason}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    for arg in ("base", "head", "merge"):
        parser.add_argument("--" + arg, required=True)
    args = parser.parse_args()
    try:
        result = select(args.repo, args.base, args.head, args.merge, os.environ["PR_TITLE"])
    except (KeyError, ValueError, subprocess.SubprocessError) as exc:
        parser.exit(1, f"Cannot verify Databricks selection: {exc}\n")
    print(json.dumps(result, separators=(",", ":")))

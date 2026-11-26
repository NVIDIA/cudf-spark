# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Choose the hosted Maven checks for the tested PR merge."""

from fnmatch import fnmatchcase
import json
import os
from pathlib import Path, PurePosixPath
import subprocess


def matches(path, rule):
    return fnmatchcase(path.name, rule["filename"]) and (
        str(path.parent) in rule["directories"]
        or any(PurePosixPath(tree) in path.parents for tree in rule.get("subtrees", []))
    )


def verify_mode(files, rules):
    mode = "skip"
    for filename in files:
        # Even documentation can be generated or packaged by the Maven build.
        if filename in rules["maven_inputs"]:
            return "full"
        path = PurePosixPath(filename)
        if matches(path, rules["skip"]):
            continue
        if matches(path, rules["rat"]):
            mode = "rat"
            continue
        return "full"
    return mode if files else "full"


if __name__ == "__main__":
    # Disabling rename detection checks both the removed and added paths.
    diff = subprocess.check_output(["git", "diff", "--no-renames", "--name-only", "-z", "HEAD^1", "HEAD"])
    rules = json.loads(Path(__file__).with_name("mvn-verify-paths.json").read_text())
    print(verify_mode([os.fsdecode(p) for p in diff.split(b"\0") if p], rules))

#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Copy shared eval resources into each skill's evals/ directory"""

import shutil
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SKILLS_DIR = REPO_ROOT / "skills"
RESOURCES_DIR = SKILLS_DIR / "evals" / "resources"

# Reuse the pre-merge CI image for evals.
SRC_DOCKERFILE = SKILLS_DIR / "ci" / "Dockerfile.pre-merge"

# Inserted into the generated Dockerfile copy, after the license header.
# Only the Dockerfile gets this note because it is purely human-facing.
# Since code files are read by the eval agent, this note could mislead the agent.
GENERATED_NOTE = """
###
# AUTO-GENERATED from skills/ci/Dockerfile.pre-merge by skills/scripts/sync_eval_resources.py.
# Do not edit this copy directly. Edit the source and re-run the sync script.
###
"""

# The prerequisite resources each skill stages for its eval, keyed by task.
# The files are copied by basename (directories are flattened).
MANIFESTS: dict[str, dict[str, list[str]]] = {
    "array-segment-sum": {
        "udf-gen-test": ["common/ArraySegmentSumUDF.scala"],
        "udf-convert-to-cudf": ["common/ArraySegmentSumUDF.scala", "common/UnitTest.scala"],
        "udf-convert-to-sql": ["common/ArraySegmentSumUDF.scala", "common/UnitTest.scala"],
        "udf-convert-to-cuda": ["common/ArraySegmentSumUDF.scala", "common/UnitTest.scala"],
        "udf-benchmark": [
            "common/ArraySegmentSumUDF.scala",
            "common/UnitTest.scala",
            "common/ArraySegmentSumRapidsUDF.scala",
        ],
        "udf-optimize-cudf": [
            "common/ArraySegmentSumUDF.scala",
            "common/UnitTest.scala",
            "common/ArraySegmentSumRapidsUDF.scala",
        ],
        # the tests under judge/ are intentionally erroneous to exercise a failing verdict
        "udf-judge-conversion": [
            "common/ArraySegmentSumUDF.scala",
            "judge/UnitTest.scala",
            "common/ArraySegmentSumRapidsUDF.scala",
            "judge/CudfComparisonTest.scala",
        ],
    },
}


def sync_task_files(task: str, skill: str, files: list[str]) -> None:
    src_dir = RESOURCES_DIR / task
    dest = SKILLS_DIR / skill / "evals" / "files" / task
    dest.mkdir(parents=True, exist_ok=True)
    basenames = [Path(f).name for f in files]
    dupes = [n for n, c in Counter(basenames).items() if c > 1]
    if dupes:
        raise ValueError(f"{skill}/{task}: duplicate basenames in manifest: {dupes}")

    wanted = set(basenames)
    # drop anything in the destination that is no longer in the manifest
    for p in dest.glob("*"):
        if p.is_file() and p.name not in wanted:
            p.unlink()
            print(f"  removed {skill}/evals/files/{task}/{p.name}")
    for f in files:
        shutil.copy2(src_dir / f, dest / Path(f).name)

    print(f"  synced {skill} ({task}):")
    print("    " + "\n    ".join(files))


def prune_stale_tasks(skill: str, tasks: set[str]) -> None:
    """Remove evals/files/<task>/ dirs for tasks that no longer stage this skill."""
    files_dir = SKILLS_DIR / skill / "evals" / "files"
    if not files_dir.is_dir():
        return
    for p in files_dir.iterdir():
        if p.is_dir() and p.name not in tasks:
            shutil.rmtree(p)
            print(f"  removed stale task dir {skill}/evals/files/{p.name}")


def write_env_dockerfile(skill: str) -> None:
    """Copy the CI Dockerfile, inserting the generated-file note after the license header."""
    lines = SRC_DOCKERFILE.read_text(encoding="utf-8").splitlines(keepends=True)
    idx = next(
        (i + 1 for i, ln in enumerate(lines) if ln.startswith("# SPDX-License-Identifier")),
        0,
    )
    env_dir = SKILLS_DIR / skill / "evals" / "environment"
    env_dir.mkdir(parents=True, exist_ok=True)
    (env_dir / "Dockerfile").write_text(
        "".join(lines[:idx]) + GENERATED_NOTE + "".join(lines[idx:]), encoding="utf-8"
    )
    print(f"  synced {skill}: environment/Dockerfile")


def main() -> None:
    # skill -> set of tasks that stage resources for it
    skill_tasks: dict[str, set[str]] = {}
    for task, manifest in MANIFESTS.items():
        for skill in manifest:
            skill_tasks.setdefault(skill, set()).add(task)

    for task, manifest in MANIFESTS.items():
        for skill, files in manifest.items():
            sync_task_files(task, skill, files)

    for skill, tasks in skill_tasks.items():
        prune_stale_tasks(skill, tasks)
        write_env_dockerfile(skill)


if __name__ == "__main__":
    main()

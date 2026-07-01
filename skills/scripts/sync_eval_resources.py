#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Copy shared eval resources into each skill's evals/ directory"""

import shutil
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SKILLS_DIR = REPO_ROOT / "skills"

TASK = "array-segment-sum"
SRC_DIR = SKILLS_DIR / "evals" / "resources" / TASK
DEST_TAIL = Path("evals") / "files" / TASK

# Reuse the per-merge CI image for evals.
SRC_DOCKERFILE = SKILLS_DIR / "ci" / "Dockerfile.pre-merge"

# The prerequisite resources each skill stages for its eval.
# The files are copied by basename (directories are flattened).
MANIFEST: dict[str, list[str]] = {
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
}


def main() -> None:
    for skill, files in MANIFEST.items():
        dest = SKILLS_DIR / skill / DEST_TAIL
        dest.mkdir(parents=True, exist_ok=True)
        basenames = [Path(f).name for f in files]
        dupes = [n for n, c in Counter(basenames).items() if c > 1]
        if dupes:
            raise ValueError(f"{skill}: duplicate basenames in manifest: {dupes}")

        wanted = set(basenames)
        # drop anything in the destination that is no longer in the manifest
        for p in dest.glob("*"):
            if p.is_file() and p.name not in wanted:
                p.unlink()
                print(f"  removed {skill}/{DEST_TAIL}/{p.name}")
        for f in files:
            shutil.copy2(SRC_DIR / f, dest / Path(f).name)

        env_dir = SKILLS_DIR / skill / "evals" / "environment"
        env_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(SRC_DOCKERFILE, env_dir / "Dockerfile")

        print(f"  synced {skill}:")
        print("    " + "\n    ".join(files))
        print("    " + "environment/Dockerfile")


if __name__ == "__main__":
    main()

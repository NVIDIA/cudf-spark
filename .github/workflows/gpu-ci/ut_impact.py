# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Select standalone ScalaTest suites from a complete base-to-merge diff.

Examples (paths relative to tests/src/test/scala/com/nvidia/spark/rapids/):
  OrcScanRetrySuite.scala (edit an assertion) -> ["com.nvidia.spark.rapids.OrcScanRetrySuite"]
  OrcScanSuite.scala and OrcScanRetrySuite.scala (edit assertions in both)
    -> ["com.nvidia.spark.rapids.OrcScanRetrySuite", "com.nvidia.spark.rapids.OrcScanSuite"]
  OrcScanSuite.scala and the root pom.xml -> [] (full workload, not a skip)
"""

import re
import subprocess


SUITE_PATH = re.compile(r"tests/src/test/scala/((?:[a-zA-Z_]\w*/)+)([A-Z]\w*Suite)\.scala", re.ASCII)
BASE_TYPES = r"(?:AnyFunSuite|SparkQueryCompareTestSuite|RmmSparkRetrySuiteBase)"


def suite_header(source, package, name):
    # Recognize a small file shape, not arbitrary Scala. Counting even words in comments or
    # strings may reject a valid suite, but cannot hide a second type or companion object.
    if (re.findall(r"\b(?:package|class|object|trait)\b", source) != ["package", "class"]
            or not re.search(rf"^package {re.escape(package)}$", source, re.MULTILINE)
            or re.search(r"\\u+|(?m:^[ \t]*@)|\b(?:DoNotDiscover|nestedSuites)\b", source)):
        return None
    declaration = re.search(rf"^(?:final )?class {name}\s+extends\s+{BASE_TYPES}"
                            r"(?:\s+with\s+[A-Za-z_]\w*)*\s*\{[ \t]*$", source, re.MULTILINE)
    if declaration and re.search(r"^\s+test\(", source[declaration.end():], re.MULTILINE):
        return source[:declaration.end()]
    return None


def affected_suites(root, base, merge, paths):
    """Return sorted leaf suite FQCNs. An empty list means run the full plan, never skip."""
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=root).decode("utf-8")

    suites = []
    try:
        for path in paths:
            match = SUITE_PATH.fullmatch(path)
            if not match:
                return []
            package, name = match[1][:-1].replace("/", "."), match[2]
            old, new = (suite_header(git("show", f"{rev}:{path}"), package, name) for rev in (base, merge))
            if old is None or old != new:
                return []
            # Include runtime scripts/config: e.g. filecache-enabled and locale-specific invocations.
            # This module's examples and human-readable docs are not suite consumers.
            refs = subprocess.run(["git", "grep", "-q", "-I", "-F", "-w", "-e", name, base, merge, "--",
                                   f":(exclude){path}", ":(exclude).github/workflows/gpu-ci/ut_impact.py",
                                   ":(exclude)*.md", ":(exclude)*.rst", ":(exclude)*.adoc"], cwd=root)
            if refs.returncode != 1:  # A reference or a failed lookup keeps full coverage.
                return []
            suites.append(f"{package}.{name}")
    except (OSError, UnicodeError, subprocess.CalledProcessError):
        return []
    return sorted(suites)

# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Stage only this shim's ScalaTest JUnit evidence, including failed and parallel runs."""

import os
from pathlib import Path
import shutil
import sys
from xml.etree import ElementTree as ET


def stage(reports, destination, filecache=False, expected=None):
    cases = skipped = failures = invalid = 0
    observed = set()
    executed = set()
    for path in sorted(reports.rglob("TEST-*.xml")):
        if path.resolve() != path or not path.is_file():
            raise ValueError(f"Refusing to upload a linked JUnit report: {path}")
        target = destination / path.relative_to(reports)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
        try:
            suite = ET.parse(target).getroot()
        except ET.ParseError as exc:
            print(f"Invalid JUnit report {path}: {exc}", file=sys.stderr)
            invalid += 1
            continue
        tests = list(suite.iter("testcase"))
        if expected and (suite.tag != "testsuite" or suite.get("tests") != str(len(tests)) or
                         any(suite.get(field) != "0" for field in ("errors", "failures"))):
            print(f"Incomplete or failed ScalaTest JUnit report: {path}", file=sys.stderr)
            invalid += 1
        cases += len(tests)
        skipped += sum(test.find("skipped") is not None for test in tests)
        failures += sum(1 for tag in ("failure", "error") for _ in suite.iter(tag))
        observed.update(test.get("classname", "") for test in tests)
        executed.update(test.get("classname", "") for test in tests if test.find("skipped") is None)
    print(f"{reports.relative_to(Path.cwd())}: {cases} testcases, {skipped} skipped, "
          f"{failures} failures/errors, {invalid} invalid reports")
    if failures or invalid:
        return 1
    if expected:
        if expected != observed:
            print(f"ScalaTest suite mismatch: missing={sorted(expected - observed)}, "
                  f"unexpected={sorted(observed - expected)}", file=sys.stderr)
            return 1
        if expected - executed:
            print(f"All reported cases skipped for: {sorted(expected - executed)}")
            return 3  # The worker can run this shim in full, but only if Maven succeeded.
    return int(cases <= skipped or (filecache and not any(
        name.endswith(".FileCacheIntegrationSuite") for name in executed)))


def main():
    scala, shim, group, *suites = sys.argv[1:]
    module = Path.cwd() / ("scala2.13/tests" if scala == "2.13" else "tests")
    build = module / "target" / f"spark{shim}"
    destination = Path(os.environ["RUNNER_TEMP"]) / "unit-junit" / f"spark{shim}"
    if suites:
        sys.exit(stage(build / "surefire-reports", destination / "selected", expected=set(suites)))
    # The entry point preserves the full suite before rerunning just FileCacheIntegrationSuite.
    filecache = group != "ut2"
    delta = (scala, shim, group) == ("2.13", "401", "scala213")
    checks = [("filecache-reports" if delta else "surefire-reports", filecache)]
    if filecache:
        checks.append(("unit-reports", False))
    results = [stage(build / name, destination / name, needs_filecache)
               for name, needs_filecache in checks]
    if delta:
        for spark in ("401", "411"):
            results.append(stage(module / "target" / f"spark{spark}" / "surefire-reports",
                                 destination / f"delta-{spark}",
                                 expected={"org.apache.spark.sql.delta.rapids.DeltaRuntimeShimSuite"}))
    if any(results):
        sys.exit(f"Missing or failed GPU unit-test evidence for Spark {shim} ({group})")


if __name__ == "__main__":
    main()

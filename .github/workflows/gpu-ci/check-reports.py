# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Stage only this shim's ScalaTest JUnit evidence, including failed and parallel runs."""

import os
from pathlib import Path
import shutil
import sys
from xml.etree import ElementTree as ET


def stage(reports, destination, filecache):
    cases = skipped = failures = invalid = 0
    executed_filecache = False
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
        cases += len(tests)
        skipped += sum(test.find("skipped") is not None for test in tests)
        failures += sum(1 for tag in ("failure", "error") for _ in suite.iter(tag))
        executed_filecache |= any(
            test.get("classname", "").endswith(".FileCacheIntegrationSuite") and test.find("skipped") is None
            for test in tests)
    print(f"{reports.relative_to(Path.cwd())}: {cases} testcases, {skipped} skipped, "
          f"{failures} failures/errors, {invalid} malformed reports")
    return not (failures or invalid) and cases > skipped and (not filecache or executed_filecache)


def main():
    scala, shim, group = sys.argv[1:]
    module = Path.cwd() / ("scala2.13/tests" if scala == "2.13" else "tests")
    build = module / "target" / f"spark{shim}"
    destination = Path(os.environ["RUNNER_TEMP"]) / "unit-junit" / f"spark{shim}"
    # The entry point preserves the full suite before rerunning just FileCacheIntegrationSuite.
    checks = [("surefire-reports", group != "ut2")]
    if group != "ut2":
        checks.append(("unit-reports", False))
    results = [stage(build / name, destination / name, filecache) for name, filecache in checks]
    if not all(results):
        sys.exit(f"Missing or failed GPU unit-test evidence for Spark {shim} ({group})")


if __name__ == "__main__":
    main()

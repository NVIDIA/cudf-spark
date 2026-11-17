#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Require fresh results for every selected Spark 420 migration test case."""

import argparse
import json
from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET


REPORT_GROUPS = {
    "parallel": ("wave-1", "wave-2"),
    "serial": ("ansi-off", "ansi-on"),
}
MIGRATED_SUITE_PREFIX = "org.apache.spark.sql.rapids.suites."


def expected_suites(path):
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"Duplicate manifest key {key!r} in {path}")
            result[key] = value
        return result

    manifest = json.loads(path.read_text(), object_pairs_hook=unique_object)
    suites = manifest.get("suites") if isinstance(manifest, dict) else None
    if not isinstance(suites, dict) or not suites:
        raise ValueError(f"No expected migration suites in {path}")
    for suite, cases in suites.items():
        if not re.fullmatch(r"[\w$]+(?:\.[\w$]+)+", suite):
            raise ValueError(f"Expected a fully qualified suite name, found {suite!r}")
        if not isinstance(cases, dict) or set(cases) != {"selected", "ignored"}:
            raise ValueError(f"Expected selected and ignored case lists for {suite}")
        for status, names in cases.items():
            if not isinstance(names, list) or not all(
                    isinstance(name, str) and name for name in names):
                raise ValueError(f"Invalid {status} case list for {suite}")
            if len(names) != len(set(names)):
                raise ValueError(f"Duplicate {status} case name for {suite}")
            cases[status] = set(names)
        if not cases["selected"]:
            raise ValueError(f"No selected cases for {suite}")
        if cases["selected"] & cases["ignored"]:
            raise ValueError(f"Selected and ignored case lists overlap for {suite}")
    return suites


def count_attribute(suite, attribute, path, required=False):
    value = suite.get(attribute)
    if value is None and not required:
        return None
    if value is None or not re.fullmatch(r"\d+", value):
        raise ValueError(f"Invalid {attribute} count in {path}: {value!r}")
    return int(value)


def check_reports(reports_dir, suites_file, started_after, mode):
    suites = expected_suites(suites_file)
    started_ns = started_after.stat().st_mtime_ns
    summaries = []
    for group in REPORT_GROUPS[mode]:
        group_dir = reports_dir / group
        found = set()
        for path in sorted(group_dir.rglob("TEST-*.xml")):
            root = ET.parse(path).getroot()
            if root.tag != "testsuite":
                raise ValueError(f"Expected a testsuite XML root in {path}")
            suite = root.get("name")
            if suite not in suites:
                if suite and suite.startswith(MIGRATED_SUITE_PREFIX):
                    raise ValueError(f"Unexpected migration suite {suite} in {group}; "
                                     f"update {suites_file}")
                continue
            if suite in found:
                raise ValueError(f"Duplicate report for {suite} in {group}")
            found.add(suite)
            if path.stat().st_mtime_ns < started_ns:
                raise ValueError(f"Stale report for {suite} in {group}: {path}")
            tests = count_attribute(root, "tests", path, required=True)
            failures = count_attribute(root, "failures", path, required=True)
            errors = count_attribute(root, "errors", path, required=True)
            if failures or errors or any(
                    element.tag in ("failure", "error") for element in root.iter()):
                raise ValueError(f"Failed or aborted suite {suite} in {group}: {path}")
            cases = root.findall("testcase")
            if tests != len(cases):
                raise ValueError(f"Test count does not match testcases for {suite} in {group}")
            names = [case.get("name") for case in cases]
            if not all(names):
                raise ValueError(f"Missing testcase name for {suite} in {group}")
            if len(names) != len(set(names)):
                raise ValueError(f"Duplicate testcase name for {suite} in {group}")
            skipped = sum(case.find("skipped") is not None for case in cases)
            reported_skipped = count_attribute(root, "skipped", path)
            if reported_skipped is not None and reported_skipped != skipped:
                raise ValueError(f"Skipped count does not match testcases for {suite} in {group}")
            executed = tests - skipped
            if executed <= 0:
                raise ValueError(f"No executed tests for {suite} in {group}")
            actual = {
                "selected": {case.get("name") for case in cases
                             if case.find("skipped") is None},
                "ignored": {case.get("name") for case in cases
                            if case.find("skipped") is not None},
            }
            for status, expected in suites[suite].items():
                missing = expected - actual[status]
                unexpected = actual[status] - expected
                if missing or unexpected:
                    raise ValueError(
                        f"{status.capitalize()} cases differ for {suite} in {group}: "
                        f"missing={sorted(missing)!r}, unexpected={sorted(unexpected)!r}")
            summaries.append(f"{group}: {suite}: succeeded {executed}, skipped {skipped}")
        missing = set(suites) - found
        if missing:
            raise ValueError(f"Missing migration suites in {group}: {', '.join(sorted(missing))}")
    return summaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports-dir", type=Path, required=True)
    parser.add_argument("--expected-suites", type=Path, required=True)
    parser.add_argument("--started-after", type=Path, required=True,
                        help="Marker created before this clean Maven test run, outside target/")
    parser.add_argument("--mode", choices=REPORT_GROUPS, required=True)
    args = parser.parse_args()
    try:
        summaries = check_reports(args.reports_dir, args.expected_suites,
                                  args.started_after, args.mode)
    except (OSError, ValueError, ET.ParseError) as error:
        print(f"Migration UT report gate failed: {error}", file=sys.stderr)
        return 1
    for summary in summaries:
        print(summary)
    print("Migration UT report gate passed for both ANSI modes")
    return 0


if __name__ == "__main__":
    sys.exit(main())

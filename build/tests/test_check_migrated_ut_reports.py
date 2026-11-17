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

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "build"))

from check_migrated_ut_reports import check_reports, REPORT_GROUPS


class MigrationReportGateTest(unittest.TestCase):
    SUITE = "org.apache.spark.sql.rapids.suites.RapidsExampleSuite"

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.expected = self.root / "expected.json"
        self.manifest()
        self.marker = self.root / "started"
        self.marker.touch()
        self.started_ns = self.marker.stat().st_mtime_ns

    def manifest(self, selected=None, ignored=None):
        self.expected.write_text(json.dumps({"suites": {self.SUITE: {
            "selected": ["works"] if selected is None else selected,
            "ignored": [] if ignored is None else ignored,
        }}}))

    def report(self, mode="parallel", group=None, cases="<testcase name='works'/>",
               tests=1, failures=0, errors=0, skipped=None, extra="", suffix=""):
        paths = []
        for report_group in ([group] if group else REPORT_GROUPS[mode]):
            path = self.root / "reports" / report_group / f"TEST-{self.SUITE}{suffix}.xml"
            path.parent.mkdir(parents=True, exist_ok=True)
            skip_attribute = f" skipped='{skipped}'" if skipped is not None else ""
            path.write_text(
                f"<testsuite name='{self.SUITE}' tests='{tests}' failures='{failures}' "
                f"errors='{errors}'{skip_attribute}>{cases}{extra}</testsuite>")
            os.utime(path, ns=(self.started_ns + 1, self.started_ns + 1))
            paths.append(path)
        return paths

    def check(self, mode="parallel"):
        return check_reports(self.root / "reports", self.expected, self.marker, mode)

    def test_parallel_and_serial_pass(self):
        for mode in REPORT_GROUPS:
            with self.subTest(mode=mode):
                self.report(mode=mode)
                self.assertEqual(2, len(self.check(mode)))

    def test_ignored_tests_do_not_count_as_execution(self):
        self.manifest(ignored=["ignored"])
        self.report(cases="<testcase name='works'/><testcase name='ignored'><skipped/>"
                          "</testcase>", tests=2, skipped=1)
        self.assertTrue(all("succeeded 1, skipped 1" in line for line in self.check()))

    def test_no_expected_suites_fails(self):
        self.expected.write_text('{"suites": {}}')
        with self.assertRaisesRegex(ValueError, "No expected"):
            self.check()

    def test_missing_suite_fails(self):
        self.report()
        manifest = json.loads(self.expected.read_text())
        manifest["suites"]["org.apache.spark.sql.rapids.suites.RapidsMissingSuite"] = {
            "selected": ["missing"], "ignored": []}
        self.expected.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "Missing migration suites"):
            self.check()

    def test_missing_ansi_wave_fails(self):
        self.report(group="wave-1")
        with self.assertRaisesRegex(ValueError, "Missing migration suites in wave-2"):
            self.check()

    def test_unlisted_migration_suite_fails(self):
        self.report()
        path = self.report(group="wave-1", suffix="-extra")[0]
        path.write_text(path.read_text().replace(
            self.SUITE, "org.apache.spark.sql.rapids.suites.RapidsUnlistedSuite"))
        with self.assertRaisesRegex(ValueError, "Unexpected migration suite"):
            self.check()

    def test_unrelated_plugin_suite_is_not_a_migration(self):
        self.report()
        path = self.report(group="wave-1", suffix="-plugin")[0]
        path.write_text(path.read_text().replace(
            self.SUITE, "com.nvidia.spark.rapids.PluginExampleSuite"))
        self.assertEqual(2, len(self.check()))

    def test_stale_report_fails(self):
        paths = self.report()
        os.utime(paths[0], ns=(self.started_ns - 1, self.started_ns - 1))
        with self.assertRaisesRegex(ValueError, "Stale report"):
            self.check()

    def test_zero_tests_and_all_skipped_fail(self):
        for cases, count in [("", 0), ("<testcase name='canceled'><skipped/></testcase>", 1)]:
            with self.subTest(cases=cases):
                self.report(cases=cases, tests=count)
                with self.assertRaisesRegex(ValueError, "No executed tests"):
                    self.check()

    def test_failure_and_abort_fail(self):
        for fields in [{"failures": 1}, {"errors": 1},
                       {"extra": "<error message='suite aborted'/>"},
                       {"cases": "<testcase><failure/></testcase>"}]:
            with self.subTest(fields=fields):
                self.report(**fields)
                with self.assertRaisesRegex(ValueError, "Failed or aborted suite"):
                    self.check()

    def test_duplicate_report_fails(self):
        self.report()
        self.report(group="wave-1", suffix="-copy")
        with self.assertRaisesRegex(ValueError, "Duplicate report"):
            self.check()

    def test_count_mismatch_fails(self):
        self.report(tests=2)
        with self.assertRaisesRegex(ValueError, "Test count does not match"):
            self.check()

    def test_skipped_count_mismatch_fails(self):
        self.report(skipped=1)
        with self.assertRaisesRegex(ValueError, "Skipped count does not match"):
            self.check()

    def test_selected_case_filtered_out_fails(self):
        self.manifest(selected=["works", "filtered out"])
        self.report()
        with self.assertRaisesRegex(ValueError, "Selected cases differ.*filtered out"):
            self.check()

    def test_unexpected_case_fails(self):
        self.report(tests=2, cases="<testcase name='works'/><testcase name='unexpected'/>")
        with self.assertRaisesRegex(ValueError, "Selected cases differ.*unexpected"):
            self.check()

    def test_duplicate_case_name_fails(self):
        self.report(tests=2, cases="<testcase name='works'/><testcase name='works'/>")
        with self.assertRaisesRegex(ValueError, "Duplicate testcase name"):
            self.check()

    def test_partially_skipped_selected_case_fails(self):
        self.manifest(selected=["works", "canceled"])
        self.report(tests=2, cases="<testcase name='works'/><testcase name='canceled'>"
                                  "<skipped/></testcase>")
        with self.assertRaisesRegex(ValueError, "Selected cases differ.*canceled"):
            self.check()

    def test_ignored_case_must_remain_skipped(self):
        self.manifest(ignored=["ignored"])
        self.report(tests=2, cases="<testcase name='works'/><testcase name='ignored'/>")
        with self.assertRaisesRegex(ValueError, "Selected cases differ.*ignored"):
            self.check()

    def test_missing_ignored_case_fails(self):
        self.manifest(ignored=["ignored"])
        self.report()
        with self.assertRaisesRegex(ValueError, "Ignored cases differ.*ignored"):
            self.check()

    def test_duplicate_or_overlapping_expected_cases_fail(self):
        for selected, ignored, message in [
                (["works", "works"], [], "Duplicate selected case name"),
                (["works"], ["works"], "case lists overlap"),
                ([], [], "No selected cases")]:
            with self.subTest(selected=selected, ignored=ignored):
                self.manifest(selected, ignored)
                with self.assertRaisesRegex(ValueError, message):
                    self.check()

    def test_duplicate_manifest_key_fails(self):
        self.expected.write_text('{"suites": {}, "suites": {}}')
        with self.assertRaisesRegex(ValueError, "Duplicate manifest key"):
            self.check()


if __name__ == "__main__":
    unittest.main()

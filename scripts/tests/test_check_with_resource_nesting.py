#!/usr/bin/env python

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

from __future__ import print_function

import collections
import contextlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest


try:
    TEXT_TYPE = unicode
except NameError:  # Python 3
    TEXT_TYPE = str


SCRIPT = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                      "check_with_resource_nesting.py")
try:
    import importlib.util
    SPEC = importlib.util.spec_from_file_location("check_with_resource_nesting", SCRIPT)
    LINT = importlib.util.module_from_spec(SPEC)
    sys.modules[SPEC.name] = LINT
    SPEC.loader.exec_module(LINT)
except ImportError:  # Jython 2.7 / Python 2.7
    import imp
    LINT = imp.load_source("check_with_resource_nesting", SCRIPT)


@contextlib.contextmanager
def temporary_directory():
    path = tempfile.mkdtemp()
    try:
        yield path
    finally:
        shutil.rmtree(path)


class OutputSink(object):
    def __init__(self):
        self.parts = []

    def write(self, value):
        self.parts.append(value)

    def flush(self):
        pass

    def getvalue(self):
        return TEXT_TYPE("").join(self.parts)


@contextlib.contextmanager
def captured_stream(name):
    output = OutputSink()
    original = getattr(sys, name)
    setattr(sys, name, output)
    try:
        yield output
    finally:
        setattr(sys, name, original)


def write_text(path, value):
    with io.open(path, "w", encoding="utf-8") as output_file:
        output_file.write(TEXT_TYPE(value))


def nested_source(depth):
    body = "result"
    for index in reversed(range(depth)):
        body = "withResource(make{0}()) {{ resource{0} =>\n{1}\n}}".format(
            index, body)
    return body


class WithResourceNestingLintSuite(unittest.TestCase):
    def test_allows_nesting_at_limit(self):
        result = LINT.scan_source("Test.scala", nested_source(4), 4)
        self.assertEqual((), result.violations)

    def test_reports_each_scope_beyond_limit(self):
        result = LINT.scan_source("Test.scala", nested_source(6), 4)
        self.assertEqual([5, 6], [violation.depth for violation in result.violations])

    def test_checks_calls_with_explicit_type_arguments(self):
        source = nested_source(5).replace(
            "withResource", "Arm.withResource[AutoCloseable, Any]")
        result = LINT.scan_source("Test.scala", source, 4)
        self.assertEqual([5], [violation.depth for violation in result.violations])

    def test_checks_parenthesized_callback(self):
        source = """
          withResource(make0()) { resource0 =>
            withResource(make1()) { resource1 =>
              withResource(make2()) { resource2 =>
                withResource(make3()) { resource3 =>
                  withResource(make4())(_ => result)
                }
              }
            }
          }
        """
        result = LINT.scan_source("Test.scala", source, 4)
        self.assertEqual([5], [violation.depth for violation in result.violations])

    def test_checks_nesting_inside_parenthesized_callback(self):
        source = """
          withResource(make0()) { resource0 =>
            withResource(make1()) { resource1 =>
              withResource(make2()) { resource2 =>
                withResource(make3())(_ =>
                  withResource(make4()) { resource4 => result })
              }
            }
          }
        """
        result = LINT.scan_source("Test.scala", source, 4)
        self.assertEqual([5], [violation.depth for violation in result.violations])

    def test_ignores_comments_and_literals(self):
        source = '''
          // withResource(commented()) {
          val text = "withResource(notCode()) { }"
          val regex = """withResource(notCodeEither()) { }"""
        ''' + nested_source(4)
        result = LINT.scan_source("Test.scala", source, 4)
        self.assertEqual((), result.violations)

    def test_ignores_directives_in_block_comments_and_literals(self):
        source = '''
          val text = "// with-resource-lint: allow-deep-nesting -- short"
          /*
           // with-resource-lint: allow-deep-nesting -- required by https://github.com/NVIDIA/cudf-spark/issues/11713
          */
        ''' + nested_source(5)
        result = LINT.scan_source("Test.scala", source, 4)
        self.assertEqual(1, len(result.violations))
        self.assertEqual((), result.directive_errors)

    def test_checks_interpolated_expression_bodies(self):
        source = """
          withResource(make0()) { resource0 =>
            withResource(make1()) { resource1 =>
              withResource(make2()) { resource2 =>
                withResource(make3()) { resource3 =>
                  val text = s"value=${withResource(make4()) { resource4 => resource4 }}"
                }
              }
            }
          }
        """
        result = LINT.scan_source("Test.scala", source, 4)
        self.assertEqual([5], [violation.depth for violation in result.violations])

    def test_handles_blocks_inside_resource_argument(self):
        source = """
          withResource(values.map { value => convert(value) }) { first =>
            withResource(make2()) { second =>
              withResource(make3()) { third =>
                withResource(make4()) { fourth =>
                  withResource(make5()) { fifth => fifth }
                }
              }
            }
          }
        """
        result = LINT.scan_source("Test.scala", source, 4)
        self.assertEqual([5], [violation.depth for violation in result.violations])

    def test_justified_directive_exempts_subtree(self):
        source = """
          // with-resource-lint: allow-deep-nesting -- required by https://github.com/NVIDIA/cudf-spark/issues/11713
        """ + nested_source(6)
        result = LINT.scan_source("Test.scala", source, 4)
        self.assertEqual((), result.violations)
        self.assertEqual((), result.directive_errors)

    def test_directive_requires_a_reason(self):
        source = """
          // with-resource-lint: allow-deep-nesting -- short
        """ + nested_source(5)
        result = LINT.scan_source("Test.scala", source, 4)
        self.assertEqual(1, len(result.directive_errors))

    def test_directive_requires_an_issue_link(self):
        source = """
          // with-resource-lint: allow-deep-nesting -- required by the native API
        """ + nested_source(5)
        result = LINT.scan_source("Test.scala", source, 4)
        self.assertEqual(1, len(result.directive_errors))

    def test_directive_accepts_short_issue_reference(self):
        source = """
          // with-resource-lint: allow-deep-nesting -- required by #11713
        """ + nested_source(5)
        result = LINT.scan_source("Test.scala", source, 4)
        self.assertEqual((), result.violations)
        self.assertEqual((), result.directive_errors)

    def test_directive_rejects_malformed_issue_suffix(self):
        source = """
          // with-resource-lint: allow-deep-nesting -- see xhttps://github.com/NVIDIA/cudf-spark/issues/7zzz
        """ + nested_source(5)
        result = LINT.scan_source("Test.scala", source, 4)
        self.assertEqual(1, len(result.directive_errors))

    def test_baseline_allows_only_recorded_occurrences(self):
        result = LINT.scan_source("Test.scala", nested_source(5), 4)
        violation = result.violations[0]
        baseline = collections.Counter({violation.baseline_key: 1})
        self.assertEqual([], LINT.new_violations(result.violations, baseline))

        doubled = list(result.violations) + list(result.violations)
        self.assertEqual(1, len(LINT.new_violations(doubled, baseline)))

    def test_detects_resolved_baseline_entries(self):
        result = LINT.scan_source("Test.scala", nested_source(5), 4)
        violation = result.violations[0]
        baseline = collections.Counter({violation.baseline_key: 2})
        stale = LINT.stale_baseline_entries(result.violations, baseline)
        self.assertEqual(1, sum(stale.values()))

    def test_fingerprint_does_not_depend_on_depth(self):
        deep = LINT.scan_source("Test.scala", nested_source(5), 4).violations[0]
        shallower = LINT.scan_source("Test.scala", nested_source(5), 3).violations[-1]
        self.assertEqual(deep.fingerprint, shallower.fingerprint)

    def test_baseline_json_is_stable_across_runtimes(self):
        violation = LINT.scan_source(
            "Test.scala", "withResource(make()) { resource => result }", 0).violations[0]
        self.assertEqual("ba163e5cbee380205ebb", violation.fingerprint)
        self.assertEqual("""{
  "version": 1,
  "maxDepth": 0,
  "trackingIssue": "https://github.com/NVIDIA/cudf-spark/issues/11713",
  "entries": [
    {
      "path": "Test.scala",
      "fingerprint": "ba163e5cbee380205ebb",
      "resource": "withResource(make())"
    }
  ]
}
""", LINT.baseline_json((violation,), 0))

    def test_production_source_discovery_excludes_generated_and_test_trees(self):
        with temporary_directory() as root:
            relative_paths = (
                "module/src/main/scala/Keep.scala",
                "module/src/test/scala/IgnoreTest.scala",
                "module/target/generated/src/main/scala/IgnoreTarget.scala",
                "scala2.13/module/src/main/scala/IgnoreGeneratedPomTree.scala",
            )
            for relative_path in relative_paths:
                path = os.path.join(root, *relative_path.split("/"))
                parent = os.path.dirname(path)
                if not os.path.isdir(parent):
                    os.makedirs(parent)
                write_text(path, "object Fixture\n")

            discovered = [
                os.path.relpath(path, root).replace(os.sep, "/")
                for path in LINT.production_scala_files(root)
            ]
            self.assertEqual(["module/src/main/scala/Keep.scala"], discovered)

    def test_report_classifies_and_lists_every_violation(self):
        violations = LINT.scan_source("Test.scala", nested_source(6), 4).violations
        baseline = collections.Counter({violations[0].baseline_key: 1})
        classified = LINT.classify_violations(violations, baseline)
        self.assertEqual(["baselined", "new"], [item.status for item in classified])

        summary = LINT.render_summary(classified, collections.Counter(), (), 4)
        self.assertIn("Found 2 scope(s) deeper than 4: 1 baselined, 1 new", summary)
        for violation in violations:
            self.assertIn(violation.resource, summary)

        with temporary_directory() as root:
            report_path = os.path.join(root, "report.json")
            LINT.write_raw_report(
                report_path, classified, collections.Counter(), (), 4)
            with io.open(report_path, "r", encoding="utf-8") as report_file:
                report = json.loads(report_file.read())
        self.assertEqual(2, len(report["violations"]))
        self.assertEqual(["baselined", "new"], [
            violation["status"] for violation in report["violations"]])
        self.assertEqual("&lt;literal&gt; &amp; value \\| next",
                         LINT.markdown_escape("<literal> & value | next"))

    def test_annotations_distinguish_baselined_and_new_violations(self):
        violations = LINT.scan_source("Test:File.scala", nested_source(6), 4).violations
        baseline = collections.Counter({violations[0].baseline_key: 1})
        classified = LINT.classify_violations(violations, baseline)
        with captured_stream("stdout") as stdout:
            LINT.emit_annotations(classified)
        output = stdout.getvalue()
        self.assertIn("::warning file=Test%3AFile.scala", output)
        self.assertIn("::error file=Test%3AFile.scala", output)

    def test_command_fails_for_new_violation(self):
        with temporary_directory() as root:
            source_dir = os.path.join(root, "module", "src", "main", "scala")
            os.makedirs(source_dir)
            write_text(os.path.join(source_dir, "Test.scala"), nested_source(5))
            baseline = os.path.join(root, "baseline.json")
            write_text(baseline, json.dumps({
                "version": 1,
                "maxDepth": 4,
                "trackingIssue": "https://github.com/NVIDIA/cudf-spark/issues/11713",
                "entries": [],
            }))

            with captured_stream("stderr") as stderr:
                exit_code = LINT.main([
                    "--root", root,
                    "--baseline", baseline,
                ])

            self.assertEqual(1, exit_code)
            self.assertIn("nesting depth 5 exceeds 4", stderr.getvalue())

    def test_command_accepts_justified_exemption(self):
        with temporary_directory() as root:
            source_dir = os.path.join(root, "module", "src", "main", "scala")
            os.makedirs(source_dir)
            source = (
                "// with-resource-lint: allow-deep-nesting -- required by "
                "https://github.com/NVIDIA/cudf-spark/issues/11713\n" +
                nested_source(5))
            write_text(os.path.join(source_dir, "Test.scala"), source)
            baseline = os.path.join(root, "baseline.json")
            write_text(baseline, json.dumps({
                "version": 1,
                "maxDepth": 4,
                "trackingIssue": "https://github.com/NVIDIA/cudf-spark/issues/11713",
                "entries": [],
            }))

            with captured_stream("stdout") as stdout:
                exit_code = LINT.main([
                    "--root", root,
                    "--baseline", baseline,
                ])

            self.assertEqual(0, exit_code)
            self.assertIn("lint passed", stdout.getvalue())

    def test_command_updates_baseline(self):
        with temporary_directory() as root:
            source_dir = os.path.join(root, "module", "src", "main", "scala")
            os.makedirs(source_dir)
            write_text(os.path.join(source_dir, "Test.scala"), nested_source(5))
            baseline = os.path.join(root, "baseline.json")

            with captured_stream("stdout") as stdout:
                exit_code = LINT.main([
                    "--root", root,
                    "--baseline", baseline,
                    "--update-baseline",
                ])

            self.assertEqual(0, exit_code)
            self.assertIn("Updated", stdout.getvalue())
            with io.open(baseline, "r", encoding="utf-8") as baseline_file:
                generated = baseline_file.read()
            scan = LINT.scan_tree(root, 4)
            self.assertEqual(LINT.baseline_json(scan.violations, 4), generated)

    def test_command_writes_complete_reports(self):
        with temporary_directory() as root:
            source_dir = os.path.join(root, "module", "src", "main", "scala")
            os.makedirs(source_dir)
            write_text(os.path.join(source_dir, "Test.scala"), nested_source(6))
            scan = LINT.scan_tree(root, 4)
            baseline = os.path.join(root, "baseline.json")
            write_text(baseline, LINT.baseline_json(scan.violations, 4))
            summary = os.path.join(root, "summary.md")
            report = os.path.join(root, "report.json")

            with captured_stream("stdout"):
                exit_code = LINT.main([
                    "--root", root,
                    "--baseline", baseline,
                    "--summary", summary,
                    "--raw-report", report,
                ])

            self.assertEqual(0, exit_code)
            with io.open(summary, "r", encoding="utf-8") as summary_file:
                self.assertIn("2 baselined, 0 new", summary_file.read())
            with io.open(report, "r", encoding="utf-8") as report_file:
                report_data = json.loads(report_file.read())
            self.assertEqual(2, len(report_data["violations"]))

    def test_command_fails_when_report_cannot_be_written(self):
        with temporary_directory() as root:
            baseline = os.path.join(root, "baseline.json")
            write_text(baseline, LINT.baseline_json((), 4))
            with captured_stream("stderr") as stderr, captured_stream("stdout"):
                exit_code = LINT.main([
                    "--root", root,
                    "--baseline", baseline,
                    "--raw-report", root,
                ])
            self.assertEqual(1, exit_code)
            self.assertIn("Could not write withResource raw report", stderr.getvalue())

    def test_command_explains_fingerprint_changes(self):
        with temporary_directory() as root:
            source_dir = os.path.join(root, "module", "src", "main", "scala")
            os.makedirs(source_dir)
            source = nested_source(5)
            source_path = os.path.join(source_dir, "Test.scala")
            write_text(source_path, source)
            violation = LINT.scan_source("module/src/main/scala/Test.scala", source, 4).violations[0]
            baseline = os.path.join(root, "baseline.json")
            write_text(baseline, json.dumps({
                "version": 1,
                "maxDepth": 4,
                "trackingIssue": "https://github.com/NVIDIA/cudf-spark/issues/11713",
                "entries": [{
                    "path": violation.path,
                    "fingerprint": violation.fingerprint,
                    "resource": violation.resource,
                }],
            }))
            write_text(source_path, source.replace("make4", "renamedMake4"))

            with captured_stream("stderr") as stderr:
                exit_code = LINT.main([
                    "--root", root,
                    "--baseline", baseline,
                ])

            self.assertEqual(1, exit_code)
            self.assertIn("baselined call changed text or path", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()

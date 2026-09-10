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

import contextlib
import io
import os
import shutil
import sys
import tempfile
import unittest
import zipfile


SCRIPT = os.path.join(os.path.dirname(os.path.dirname(__file__)), "deprecation_audit.py")
try:
    import importlib.util
    SPEC = importlib.util.spec_from_file_location("deprecation_audit", SCRIPT)
    AUDIT = importlib.util.module_from_spec(SPEC)
    sys.modules[SPEC.name] = AUDIT
    SPEC.loader.exec_module(AUDIT)
except ImportError:  # Jython 2.7 / Python 2.7
    import imp
    AUDIT = imp.load_source("deprecation_audit", SCRIPT)


class CallRecorder(object):
    def __init__(self, return_value=None, side_effect=None):
        self.return_value = return_value
        self.side_effect = side_effect
        self.calls = []

    @property
    def call_count(self):
        return len(self.calls)

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        effect = self.side_effect
        if isinstance(effect, list):
            effect = effect.pop(0)
        if isinstance(effect, BaseException):
            raise effect
        if callable(effect):
            return effect(*args, **kwargs)
        return self.return_value if effect is None else effect


class FakeOpener(object):
    def __init__(self):
        self.open = CallRecorder()


class OutputSink(object):
    def __init__(self):
        self.parts = []

    def write(self, value):
        self.parts.append(value)

    def flush(self):
        pass


@contextlib.contextmanager
def patch_attribute(target, name, value):
    original = getattr(target, name)
    setattr(target, name, value)
    try:
        yield value
    finally:
        setattr(target, name, original)


@contextlib.contextmanager
def patch_environment(updates):
    original = dict(os.environ)
    os.environ.update(updates)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(original)


@contextlib.contextmanager
def temporary_directory():
    path = tempfile.mkdtemp()
    try:
        yield path
    finally:
        shutil.rmtree(path)


@contextlib.contextmanager
def captured_stdout():
    output = OutputSink()
    with patch_attribute(sys, "stdout", output):
        yield output


def request_url(request):
    return request.get_full_url()


def assert_raises_regex(test_case, exception, pattern):
    method = getattr(test_case, "assertRaisesRegex", None)
    if method is None:
        method = test_case.assertRaisesRegexp
    return method(exception, pattern)


class DeprecationAuditSuite(unittest.TestCase):
    def test_parses_scala_verbose_deprecation(self):
        log = (
            "[INFO] /workspace/sql-plugin/src/main/scala/Test.scala:42: "
            "[deprecation @ example.Test.run | "
            "origin=ai.rapids.cudf.ColumnView.oldApi | version=] "
            "method oldApi in class ColumnView is deprecated\n"
        )
        findings = AUDIT.parse_log(log, "package-tests (330)")
        self.assertEqual(1, len(findings))
        self.assertEqual(42, findings[0].line)
        self.assertEqual("ai.rapids.cudf.ColumnView.oldApi", findings[0].origin)
        self.assertEqual("NVIDIA", findings[0].owner)

    def test_parses_maven_bracket_location(self):
        log = """
[WARNING] /workspace/src/main/java/Test.java:[17,9] oldApi() has been deprecated
"""
        findings = AUDIT.parse_log(log, "verify-all-212-modules (330, 17)")
        self.assertEqual(17, findings[0].line)
        self.assertEqual("third-party/unknown", findings[0].owner)

    def test_reads_scala_213_origin_from_following_line(self):
        log = """
[INFO] /workspace/sql-plugin/src/main/scala/Test.scala:42: method oldApi is deprecated
Applicable -Wconf filters: cat=deprecation, origin=com.nvidia.spark.rapids.jni.Api.oldApi
"""
        findings = AUDIT.parse_log(log, "package-tests-scala213 (350)")
        self.assertEqual("com.nvidia.spark.rapids.jni.Api.oldApi", findings[0].origin)
        self.assertEqual("NVIDIA", findings[0].owner)

    def test_classifies_all_advisory_origins_as_nvidia(self):
        origins = (
            "ai.rapids.cudf.ColumnView.oldApi",
            "com.nvidia.spark.rapids.optimizer.OptimizerConf.oldApi",
            "org.apache.spark.sql.rapids.internal.PrivateRapidsConfs.oldApi",
            "org.apache.spark.sql.execution.aggregate.PartialAggUtils.oldApi",
            "org.apache.spark.sql.execution.aggregate.PartialAggUtils$Helper.oldApi",
        )
        for origin in origins:
            finding = AUDIT.Finding("Test.scala", 1, "deprecated", origin)
            self.assertEqual("NVIDIA", finding.owner, origin)

    def test_partial_agg_utils_owner_match_has_symbol_boundary(self):
        origins = (
            "org.apache.spark.sql.execution.aggregate.PartialAggUtilsNeighbor.oldApi",
            "org.apache.spark.sql.execution.aggregate.SparkApi.oldApi",
            "org.example.fixture.ThirdPartyApi.oldApi",
        )
        for origin in origins:
            finding = AUDIT.Finding("Test.scala", 1, "deprecated", origin)
            self.assertEqual("third-party/unknown", finding.owner, origin)

    def test_ignores_non_source_deprecation_text(self):
        log = "[WARNING] This build plugin uses a deprecated Maven feature\n"
        self.assertEqual([], AUDIT.parse_log(log, "install-modules (3.9.3)"))

    def test_merges_same_finding_across_matrix_jobs(self):
        first = AUDIT.Finding(
            "Test.scala", 1, "[deprecation] old is deprecated", "ai.rapids.cudf.Api.old",
            {"330"})
        second = AUDIT.Finding(
            "Test.scala", 1, "old is deprecated", "ai.rapids.cudf.Api.old", {"400"})
        merged = AUDIT.merge_findings([first, second])
        self.assertEqual({"330", "400"}, merged[0].jobs)

    def test_decodes_zip_job_log(self):
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w") as archive:
            archive.writestr("job/step.txt", "deprecated output")
        self.assertEqual("deprecated output", AUDIT.decode_job_log(payload.getvalue()))

    def test_job_log_redirect_does_not_forward_authorization(self):
        api_url = "https://api.github.com/repos/NVIDIA/cudf-spark/actions/jobs/123/logs"
        signed_url = "https://results-receiver.example/job.log?signature=secret"
        redirect = AUDIT.urllib_error.HTTPError(
            api_url, 302, "Found", {"Location": signed_url}, None)
        authenticated_opener = FakeOpener()
        authenticated_opener.open.side_effect = redirect
        build_opener = CallRecorder(return_value=authenticated_opener)
        signed_open = CallRecorder(return_value=io.BytesIO(b"job log"))

        with patch_attribute(AUDIT.urllib_request, "build_opener", build_opener), \
                patch_attribute(AUDIT.urllib_request, "urlopen", signed_open):
            payload = AUDIT.request_bytes(api_url, "github-token")

        self.assertEqual(b"job log", payload)
        authenticated_request = authenticated_opener.open.calls[0][0][0]
        self.assertEqual("Bearer github-token",
                         authenticated_request.get_header("Authorization"))
        signed_request = signed_open.calls[0][0][0]
        self.assertEqual(signed_url, request_url(signed_request))
        self.assertIsNone(signed_request.get_header("Authorization"))
        self.assertIsNone(signed_request.get_header("X-GitHub-Api-Version"))

    def test_job_log_redirect_requires_location(self):
        api_url = "https://api.github.com/repos/NVIDIA/cudf-spark/actions/jobs/123/logs"
        authenticated_opener = FakeOpener()
        authenticated_opener.open.side_effect = AUDIT.urllib_error.HTTPError(
            api_url, 302, "Found", {}, None)
        with patch_attribute(
                AUDIT.urllib_request, "build_opener",
                CallRecorder(return_value=authenticated_opener)), \
                assert_raises_regex(self, AUDIT.JobLogRedirectError, "Location"):
            AUDIT.request_bytes(api_url, "github-token")

    def test_job_log_redirect_rejects_non_https_location(self):
        api_url = "https://api.github.com/repos/NVIDIA/cudf-spark/actions/jobs/123/logs"
        authenticated_opener = FakeOpener()
        authenticated_opener.open.side_effect = AUDIT.urllib_error.HTTPError(
            api_url, 302, "Found", {"Location": "http://logs.example/job.log"}, None)
        with patch_attribute(
                AUDIT.urllib_request, "build_opener",
                CallRecorder(return_value=authenticated_opener)), \
                assert_raises_regex(self, AUDIT.JobLogRedirectError, "HTTPS"):
            AUDIT.request_bytes(api_url, "github-token")

    def test_job_log_endpoint_rejects_non_redirect_response(self):
        api_url = "https://api.github.com/repos/NVIDIA/cudf-spark/actions/jobs/123/logs"
        authenticated_opener = FakeOpener()
        authenticated_opener.open.return_value = io.BytesIO(b"unexpected direct response")
        with patch_attribute(
                AUDIT.urllib_request, "build_opener",
                CallRecorder(return_value=authenticated_opener)), \
                assert_raises_regex(self, AUDIT.JobLogRedirectError, "expected redirect"):
            AUDIT.request_bytes(api_url, "github-token")

    def test_download_logs_preserves_partial_results_after_bad_redirect(self):
        jobs = {"jobs": [
            {"id": 1, "name": "package-tests (330, false)", "status": "completed"},
            {"id": 2, "name": "package-tests (340, false)", "status": "completed"},
        ]}
        bad_redirect = AUDIT.JobLogRedirectError("missing redirect location")
        request_bytes = CallRecorder(
            side_effect=[b"first job log", bad_redirect, bad_redirect,
                         bad_redirect, bad_redirect])
        with patch_attribute(AUDIT, "request_json", CallRecorder(return_value=jobs)), \
                patch_attribute(AUDIT, "request_bytes", request_bytes), \
                patch_attribute(AUDIT.time, "sleep", CallRecorder()):
            logs, failures = AUDIT.download_logs(
                "https://api.github.com", "NVIDIA/cudf-spark", "run-id", "token",
                AUDIT.DEFAULT_JOB_PATTERN)

        self.assertEqual({"package-tests (330, false)": "first job log"}, logs)
        self.assertEqual(5, request_bytes.call_count)
        self.assertEqual(1, len(failures))
        self.assertIn("package-tests (340, false)", failures[0])
        self.assertIn("missing redirect location", failures[0])

    def test_summary_reports_incomplete_collection(self):
        summary = AUDIT.render_summary([], ["package-tests: log unavailable"])
        self.assertIn("No compiler deprecation diagnostics", summary)
        self.assertIn("Incomplete log collection", summary)
        self.assertIn("optional", summary)

    def test_main_fails_when_findings_are_present(self):
        log = (
            u"/workspace/sql-plugin/src/main/scala/Test.scala:42: "
            "[deprecation @ example.Test.run | "
            "origin=ai.rapids.cudf.ColumnView.oldApi | version=] deprecated\n"
        )
        with temporary_directory() as temp_dir:
            log_path = os.path.join(temp_dir, "package-tests.log")
            with io.open(log_path, "w", encoding="utf-8") as log_file:
                log_file.write(log)
            with captured_stdout():
                result = AUDIT.main([
                    "--logs-dir", temp_dir,
                    "--raw-report", os.path.join(temp_dir, "report.json"),
                ])
        self.assertEqual(1, result)

    def test_main_succeeds_when_audit_is_clean(self):
        with temporary_directory() as temp_dir:
            log_path = os.path.join(temp_dir, "package-tests.log")
            summary_path = os.path.join(temp_dir, "summary.md")
            with io.open(log_path, "w", encoding="utf-8") as log_file:
                log_file.write(u"clean build\n")
            with captured_stdout():
                result = AUDIT.main([
                    "--logs-dir", temp_dir,
                    "--summary", summary_path,
                    "--raw-report", os.path.join(temp_dir, "report.json"),
                ])
            with io.open(summary_path, "r", encoding="utf-8") as summary_file:
                self.assertIn("No compiler deprecation diagnostics", summary_file.read())
        self.assertEqual(0, result)

    def test_main_fails_when_log_collection_is_incomplete(self):
        with temporary_directory() as temp_dir, \
                patch_attribute(
                    AUDIT, "download_logs",
                    CallRecorder(return_value=({}, ["package-tests: log unavailable"]))), \
                patch_environment({"GITHUB_TOKEN": "token"}):
            with captured_stdout():
                result = AUDIT.main([
                    "--repository", "NVIDIA/cudf-spark",
                    "--run-id", "123",
                    "--logs-dir", "",
                    "--raw-report", os.path.join(temp_dir, "report.json"),
                ])
        self.assertEqual(1, result)

    def test_annotation_property_escaping(self):
        self.assertEqual("path%3Awith%2Cpunctuation", AUDIT.command_property_escape(
            "path:with,punctuation"))


if __name__ == "__main__":
    unittest.main()

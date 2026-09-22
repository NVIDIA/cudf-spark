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

import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "get_iceberg_versions.py"
SPEC = importlib.util.spec_from_file_location("get_iceberg_versions", SCRIPT)
MATRIX_READER = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MATRIX_READER
SPEC.loader.exec_module(MATRIX_READER)

# Only the properties consumed by the loader are needed. Keep parser/selector tests
# independent of the live release profiles, Spark shims, and Iceberg version pins.
POM_FIXTURE = """<project xmlns="http://maven.apache.org/POM/4.0.0">
  <properties>
    <spark350.version>3.5.0</spark350.version>
    <spark351.version>3.5.1</spark351.version>
    <spark354.version>3.5.4</spark354.version>
    <spark355.version>3.5.5</spark355.version>
    <spark356.version>3.5.6</spark356.version>
    <spark401.version>4.0.1</spark401.version>
    <spark420.version>4.2.0</spark420.version>
    <iceberg.version>1.6.1</iceberg.version>
  </properties>
</project>
"""


class IcebergVersionMatrixSuite(unittest.TestCase):
    def setUp(self):
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.pom_path = Path(temp_dir.name) / "pom.xml"
        self.pom_path.write_text(POM_FIXTURE, encoding="utf-8")
        self.matrix_path = Path(temp_dir.name) / "matrix.json"
        self.document = {
            "iceberg_versions": [
                {
                    "version": "1.6.1",
                    "upstream_minimums": {"3.5": "3.5.1"},
                    "spark_versions": [
                        {"version": "3.5.1", "supported": True},
                        {"version": "3.5.4", "supported": False,
                         "reason": "Iceberg 1.6.x is not packaged for this Spark shim"},
                        {"version": "3.5.5", "supported": False,
                         "reason": "Iceberg 1.6.x is not packaged for this Spark shim"},
                        {"version": "3.5.6", "supported": False,
                         "reason": "Iceberg 1.6.x is not packaged for this Spark shim"},
                    ],
                },
                {
                    "version": "1.9.2",
                    "upstream_minimums": {"3.5": "3.5.5"},
                    "spark_versions": [
                        {"version": "3.5.5", "supported": True},
                        {"version": "3.5.6", "supported": True},
                    ],
                },
                {
                    "version": "1.10.1",
                    "upstream_minimums": {"3.5": "3.5.6", "4.0": "4.0.0"},
                    "spark_versions": [
                        {"version": "3.5.6", "supported": True},
                        {"version": "4.0.1", "supported": True},
                    ],
                },
            ],
        }
        self.matrix = self._load_document(self.document)

    def _load_document(self, document):
        self.matrix_path.write_text(json.dumps(document), encoding="utf-8")
        return MATRIX_READER.IcebergVersionMatrix.load(self.matrix_path, self.pom_path)

    def _run_cli(self, *arguments):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = MATRIX_READER.main([
                "--matrix", str(self.matrix_path), "--pom", str(self.pom_path), *arguments])
        return exit_code, stdout.getvalue(), stderr.getvalue()

    def test_selects_representative_versions(self):
        selections = {
            "3.5.1": ["1.6.1"],
            "3.5.6": ["1.9.2", "1.10.1"],
            "4.0.1": ["1.10.1"],
        }
        for spark_version, expected_versions in selections.items():
            with self.subTest(spark=spark_version):
                self.assertEqual(
                    expected_versions,
                    self.matrix.supported_iceberg_versions(spark_version))

    def test_minimum_baselines_intentionally_exclude_older_patches(self):
        for spark_version in ("3.5.0", "3.5.4"):
            with self.subTest(spark=spark_version):
                self.assertEqual([], self.matrix.supported_iceberg_versions(spark_version))
        self.assertEqual(["1.9.2"], self.matrix.supported_iceberg_versions("3.5.5"))

    def test_valid_requested_versions_preserve_caller_order(self):
        requested = ["1.10.1", "1.9.2"]
        self.assertEqual(
            requested,
            self.matrix.validate_requested_versions("3.5.6", requested))

    def test_unknown_requested_version_fails(self):
        with self.assertRaisesRegex(
                MATRIX_READER.MatrixError, "not present in the test matrix"):
            self.matrix.validate_requested_versions("3.5.6", ["9.9.9"])

    def test_requested_versions_outside_baseline_policy_fail(self):
        for spark_version in ("3.5.5", "4.2.0"):
            with self.subTest(spark=spark_version):
                with self.assertRaisesRegex(MATRIX_READER.MatrixError, "minimum-baseline"):
                    self.matrix.validate_requested_versions(spark_version, ["1.10.1"])

    def test_packaging_unsupported_requested_version_fails_with_reason(self):
        with self.assertRaisesRegex(
                MATRIX_READER.MatrixError, "Iceberg 1.6.x is not packaged"):
            self.matrix.validate_requested_versions("3.5.6", ["1.6.1"])

    def test_rejects_malformed_top_level_metadata(self):
        self.document["unexpected"] = True
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "must contain only"):
            self._load_document(self.document)

    def test_rejects_unsupported_entry_without_reason(self):
        del self.document["iceberg_versions"][0]["spark_versions"][1]["reason"]
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "needs a reason"):
            self._load_document(self.document)

    def test_rejects_missing_pom_derived_spark_mapping(self):
        self.document["iceberg_versions"][0]["spark_versions"].pop()
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "missing 3.5.6"):
            self._load_document(self.document)

    def test_cli_fails_for_missing_mapping_instead_of_skipping(self):
        self.document["iceberg_versions"][1]["spark_versions"].pop(0)
        self.matrix_path.write_text(json.dumps(self.document), encoding="utf-8")
        exit_code, stdout, stderr = self._run_cli("--spark-version", "3.5.5")
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("missing 3.5.5", stderr)

    def test_cli_explains_empty_selections(self):
        # 4.2.0 represents the stub family. Below-baseline patches are also valid
        # empty selections under issue #15875, even if their POM packages Iceberg.
        for spark_version in ("4.2.0", "3.5.0", "3.5.4"):
            with self.subTest(spark=spark_version):
                exit_code, stdout, stderr = self._run_cli("--spark-version", spark_version)
                self.assertEqual(0, exit_code)
                self.assertEqual("\n", stdout)
                self.assertIn(f"No Iceberg versions selected for Spark {spark_version}", stderr)
                self.assertIn("minimum-baseline and packaging test policy", stderr)

    def test_cli_validates_requested_versions(self):
        exit_code, stdout, stderr = self._run_cli(
            "--spark-version", "3.5.6", "--requested-versions", "1.10.1, 1.9.2")
        self.assertEqual(0, exit_code)
        self.assertEqual("1.10.1 1.9.2\n", stdout)
        self.assertEqual("", stderr)

    def test_cli_reports_malformed_json(self):
        self.matrix_path.write_text("{", encoding="utf-8")
        exit_code, stdout, stderr = self._run_cli("--validate")
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("Iceberg test matrix error", stderr)


if __name__ == "__main__":
    unittest.main()

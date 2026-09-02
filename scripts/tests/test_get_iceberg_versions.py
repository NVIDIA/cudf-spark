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

import copy
import importlib.util
import tempfile
import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).parents[2]
SCRIPT = REPO_ROOT / "jenkins" / "get_iceberg_versions.py"
MATRIX_PATH = REPO_ROOT / "jenkins" / "iceberg-test-matrix.yaml"
POM_PATH = REPO_ROOT / "pom.xml"
SPEC = importlib.util.spec_from_file_location("get_iceberg_versions", SCRIPT)
MATRIX = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MATRIX)


class IcebergVersionMatrixSuite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.entries = MATRIX.load_matrix(MATRIX_PATH, POM_PATH)

    def _write_modified_matrix(self, modify):
        with open(MATRIX_PATH, encoding="utf-8") as stream:
            document = yaml.safe_load(stream)
        document = copy.deepcopy(document)
        modify(document)
        temporary = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
        with temporary:
            yaml.safe_dump(document, temporary)
        path = Path(temporary.name)
        self.addCleanup(path.unlink)
        return path

    def test_returns_all_supported_iceberg_versions(self):
        expected = {
            "3.5.1": ["1.6.1"],
            "3.5.4": [],
            "3.5.5": ["1.9.2"],
            "3.5.6": ["1.9.2", "1.10.1"],
            "3.5.8": ["1.9.2", "1.10.1"],
            "4.0.2": ["1.10.1", "1.11.0"],
            "4.1.3": ["1.11.0"],
        }
        for spark_version, iceberg_versions in expected.items():
            with self.subTest(spark_version=spark_version):
                self.assertEqual(
                    iceberg_versions,
                    MATRIX.supported_iceberg_versions(self.entries, spark_version))

    def test_accepts_supported_requested_versions(self):
        requested = ["1.9.2", "1.10.1"]
        self.assertEqual(
            requested,
            MATRIX.validate_requested_versions(self.entries, "3.5.8", requested))

    def test_returns_no_versions_for_unlisted_spark(self):
        self.assertEqual([], MATRIX.supported_iceberg_versions(self.entries, "3.6.0"))

    def test_reports_reason_for_known_unsupported_combination(self):
        with self.assertRaisesRegex(
                MATRIX.MatrixError, "not currently packaged for Spark 3.5.x"):
            MATRIX.validate_requested_versions(self.entries, "3.5.8", ["1.11.0"])

    def test_rejects_combination_below_upstream_minimum(self):
        with self.assertRaisesRegex(MATRIX.MatrixError, "not upstream-compatible"):
            MATRIX.validate_requested_versions(self.entries, "3.5.4", ["1.9.2"])

    def test_rejects_unknown_iceberg_version(self):
        with self.assertRaisesRegex(MATRIX.MatrixError, "not present in the test matrix"):
            MATRIX.validate_requested_versions(self.entries, "4.0.2", ["2.0.0"])

    def test_requires_reason_for_unsupported_combination(self):
        def remove_reason(document):
            del document["iceberg_versions"][0]["spark_versions"][0]["reason"]

        path = self._write_modified_matrix(remove_reason)
        with self.assertRaisesRegex(MATRIX.MatrixError, "needs a reason"):
            MATRIX.load_matrix(path, POM_PATH)

    def test_requires_every_eligible_spark_shim(self):
        def remove_spark_version(document):
            document["iceberg_versions"][-1]["spark_versions"].pop()

        path = self._write_modified_matrix(remove_spark_version)
        with self.assertRaisesRegex(MATRIX.MatrixError, "missing 4.1.3"):
            MATRIX.load_matrix(path, POM_PATH)

    def test_rejects_duplicate_spark_version(self):
        def duplicate_spark_version(document):
            spark_versions = document["iceberg_versions"][0]["spark_versions"]
            spark_versions.append(copy.deepcopy(spark_versions[0]))

        path = self._write_modified_matrix(duplicate_spark_version)
        with self.assertRaisesRegex(MATRIX.MatrixError, "duplicate Spark version 3.3.4"):
            MATRIX.load_matrix(path, POM_PATH)


if __name__ == "__main__":
    unittest.main()

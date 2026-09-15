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
import re
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "jenkins" / "get_iceberg_versions.py"
SPEC = importlib.util.spec_from_file_location("get_iceberg_versions", SCRIPT)
MATRIX_READER = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MATRIX_READER
SPEC.loader.exec_module(MATRIX_READER)

POM_NAMESPACE = {"pom": "http://maven.apache.org/POM/4.0.0"}
PROPERTY_REFERENCE = re.compile(r"^\$\{([^}]+)\}$")

EXPECTED_RELEASE_SELECTIONS = {
    "release350": [],
    "release351": ["1.6.1"],
    "release352": ["1.6.1"],
    "release353": ["1.6.1"],
    "release354": [],
    "release355": ["1.9.2"],
    "release356": ["1.9.2", "1.10.1"],
    "release357": ["1.9.2", "1.10.1"],
    "release358": ["1.9.2", "1.10.1"],
    "release359": ["1.9.2", "1.10.1"],
    "release400": ["1.10.1"],
    "release401": ["1.10.1"],
    "release402": ["1.10.1", "1.11.0"],
    "release403": ["1.10.1", "1.11.0"],
    "release404": ["1.10.1", "1.11.0"],
    "release411": ["1.11.0"],
    "release412": ["1.11.0"],
    "release413": ["1.11.0"],
    "release420": [],
}


def _release_profiles(pom_path):
    root = ET.parse(pom_path).getroot()
    properties = {
        child.tag.rsplit("}", 1)[-1]: (child.text or "").strip()
        for child in root.find("pom:properties", POM_NAMESPACE)
    }
    profiles = {}
    for profile in root.findall("pom:profiles/pom:profile", POM_NAMESPACE):
        profile_id = profile.findtext("pom:id", namespaces=POM_NAMESPACE)
        if not re.fullmatch(r"release[0-9]+", profile_id or ""):
            continue
        spark_reference = profile.findtext(
            "pom:properties/pom:spark.version", namespaces=POM_NAMESPACE)
        match = PROPERTY_REFERENCE.fullmatch(spark_reference or "")
        if match is None:
            raise AssertionError(f"{profile_id} has an invalid Spark version reference")
        modules = [
            (module.text or "").strip()
            for module in profile.findall("pom:modules/pom:module", POM_NAMESPACE)
        ]
        profiles[profile_id] = (properties[match.group(1)], modules)
    return profiles


class IcebergVersionMatrixSuite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.matrix = MATRIX_READER.IcebergVersionMatrix.load()

    def _load_document(self, document):
        with tempfile.TemporaryDirectory() as temp_dir:
            matrix_path = Path(temp_dir) / "matrix.json"
            matrix_path.write_text(json.dumps(document), encoding="utf-8")
            return MATRIX_READER.IcebergVersionMatrix.load(matrix_path)

    def test_selects_expected_versions_for_every_release_profile(self):
        profiles = _release_profiles(MATRIX_READER.DEFAULT_POM)
        self.assertEqual(set(EXPECTED_RELEASE_SELECTIONS), set(profiles))

        for profile_id, expected_versions in EXPECTED_RELEASE_SELECTIONS.items():
            spark_version, _ = profiles[profile_id]
            with self.subTest(profile=profile_id, spark=spark_version):
                self.assertEqual(
                    expected_versions,
                    self.matrix.supported_iceberg_versions(spark_version))

    def test_stub_release_profiles_select_no_iceberg_versions(self):
        profiles = _release_profiles(MATRIX_READER.DEFAULT_POM)
        stub_profiles = {
            profile_id: spark_version
            for profile_id, (spark_version, modules) in profiles.items()
            if "iceberg/iceberg-stub" in modules
        }
        self.assertEqual({"release420": "4.2.0"}, stub_profiles)
        for spark_version in stub_profiles.values():
            self.assertEqual([], self.matrix.supported_iceberg_versions(spark_version))

    def test_valid_requested_versions_preserve_caller_order(self):
        requested = ["1.10.1", "1.9.2"]
        self.assertEqual(
            requested,
            self.matrix.validate_requested_versions("3.5.8", requested))

    def test_unknown_requested_version_fails(self):
        with self.assertRaisesRegex(
                MATRIX_READER.MatrixError, "not present in the test matrix"):
            self.matrix.validate_requested_versions("3.5.8", ["9.9.9"])

    def test_upstream_incompatible_requested_version_fails(self):
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "not upstream-compatible"):
            self.matrix.validate_requested_versions("4.0.1", ["1.9.2"])

    def test_packaging_unsupported_requested_version_fails_with_reason(self):
        with self.assertRaisesRegex(
                MATRIX_READER.MatrixError,
                "Iceberg 1.11.0 is not supported with Spark 3.5.8"):
            self.matrix.validate_requested_versions("3.5.8", ["1.11.0"])

    def test_rejects_malformed_top_level_metadata(self):
        document = json.loads(MATRIX_READER.DEFAULT_MATRIX.read_text(encoding="utf-8"))
        document["unexpected"] = True
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "must contain only"):
            self._load_document(document)

    def test_rejects_unsupported_entry_without_reason(self):
        document = json.loads(MATRIX_READER.DEFAULT_MATRIX.read_text(encoding="utf-8"))
        unsupported = next(
            support
            for iceberg in document["iceberg_versions"]
            for support in iceberg["spark_versions"]
            if not support["supported"])
        del unsupported["reason"]
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "needs a reason"):
            self._load_document(document)

    def test_rejects_missing_pom_derived_spark_mapping(self):
        document = json.loads(MATRIX_READER.DEFAULT_MATRIX.read_text(encoding="utf-8"))
        spark_versions = document["iceberg_versions"][0]["spark_versions"]
        spark_versions[:] = [
            support for support in spark_versions if support["version"] != "3.5.2"
        ]
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "missing 3.5.2"):
            self._load_document(document)

    def test_cli_succeeds_when_release_profile_has_no_selected_version(self):
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = MATRIX_READER.main(["--spark-version", "3.5.0"])
        self.assertEqual(0, exit_code)
        self.assertEqual("\n", stdout.getvalue())

    def test_cli_reports_malformed_json(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            matrix_path = Path(temp_dir) / "matrix.json"
            matrix_path.write_text("{", encoding="utf-8")
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                exit_code = MATRIX_READER.main(["--validate", "--matrix", str(matrix_path)])
        self.assertEqual(1, exit_code)
        self.assertIn("Iceberg test matrix error", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()

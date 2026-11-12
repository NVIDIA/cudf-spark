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

from __future__ import annotations

import contextlib
import copy
import io
import json
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
from scripts import get_iceberg_versions as MATRIX_READER

# Only the effective properties, profiles, and dependencies consumed by the loader are needed. Keep tests
# independent of the live release profiles, Spark shims, and Iceberg version pins.
FIXTURE_DIR = Path(__file__).resolve().parent / "resources" / "iceberg"


class IcebergVersionMatrixSuite(unittest.TestCase):
    def setUp(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.pom_path = Path(temp_dir.name) / "effective-parent.xml"
        for resource in FIXTURE_DIR.glob("effective-*.xml"):
            (self.pom_path.parent / resource.name).write_text(
                resource.read_text(), encoding="utf-8")
        mock = patch.object(MATRIX_READER, "_effective_pom", side_effect=self._maven_output)
        self.maven = mock.start()
        self.addCleanup(mock.stop)
        self.matrix_path = Path(temp_dir.name) / "matrix.json"
        self.document: Dict[str, Any] = json.loads(
            (FIXTURE_DIR / "iceberg-versions.json").read_text(encoding="utf-8"))
        self.matrix = self._load_document(self.document)

    def _maven_output(self, pom_path: Path, profile: Optional[str] = None,
                      modules: Sequence[str] = ()) -> List[ET.Element]:
        path = pom_path if profile is None else pom_path.parent / f"effective-{profile}.xml"
        root = ET.parse(path).getroot()
        return [root] if profile is None else list(root)

    def _load_document(self, document: Dict[str, Any]) -> MATRIX_READER.IcebergVersionMatrix:
        self.matrix_path.write_text(json.dumps(document), encoding="utf-8")
        matrix = MATRIX_READER.IcebergVersionMatrix.load(self.matrix_path)
        matrix.validate(self.pom_path)
        return matrix

    def _run_cli(self, command: str, *arguments: str) -> Tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            inputs = ["--matrix", str(self.matrix_path)]
            if command == "validate":
                inputs += ["--pom", str(self.pom_path)]
            exit_code = MATRIX_READER.main([command, *inputs, *arguments])
        return exit_code, stdout.getvalue(), stderr.getvalue()

    def test_selects_representative_versions(self) -> None:
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

    def test_legacy_packaged_combinations_remain_selectable(self) -> None:
        selections = {
            "3.5.0": ["1.6.1"],
            "3.5.4": ["1.9.2", "1.10.1"],
            "3.5.5": ["1.9.2", "1.10.1"],
        }
        for spark_version, expected in selections.items():
            with self.subTest(spark=spark_version):
                self.assertEqual(expected, self.matrix.supported_iceberg_versions(spark_version))
                self.assertEqual((0, " ".join(expected) + "\n", ""), self._run_cli(
                    "validate-requested", "--spark-version", spark_version,
                    "--requested-versions", " ".join(expected)))

    def test_validate_requires_each_legacy_packaged_combination(self) -> None:
        for iceberg_version, spark_version in MATRIX_READER.LEGACY_BASELINE_EXCEPTIONS:
            for missing in (True, False):
                with self.subTest(iceberg=iceberg_version, spark=spark_version, missing=missing):
                    document = copy.deepcopy(self.document)
                    iceberg = next(row for row in document["iceberg_versions"]
                                   if row["version"] == iceberg_version)
                    spark = next(row for row in iceberg["spark_versions"]
                                 if row["version"] == spark_version)
                    if missing:
                        iceberg["spark_versions"].remove(spark)
                    else:
                        spark.update(supported=False, reason="Incorrectly removed packaged support")
                    self.matrix_path.write_text(json.dumps(document), encoding="utf-8")
                    code, stdout, stderr = self._run_cli("validate")
                    self.assertEqual(1, code)
                    self.assertEqual("", stdout)
                    self.assertIn(f"Iceberg {iceberg_version}", stderr)
                    self.assertIn(spark_version, stderr)
                    self.assertIn(f"missing {spark_version}", stderr)

    def test_legacy_exceptions_require_compatibility_comments(self) -> None:
        for comment in (None, "", "  "):
            with self.subTest(comment=comment):
                document = copy.deepcopy(self.document)
                spark = document["iceberg_versions"][0]["spark_versions"][0]
                if comment is None:
                    del spark["comment"]
                else:
                    spark["comment"] = comment
                with self.assertRaisesRegex(MATRIX_READER.MatrixError, "compatibility comment"):
                    self._load_document(document)

    def test_new_iceberg_version_cannot_inherit_legacy_exceptions(self) -> None:
        self._change_effective_runtime("1.9.3")
        self.document["iceberg_versions"][1]["version"] = "1.9.3"
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "below minimum test baseline"):
            self._load_document(self.document)

    def test_comment_cannot_authorize_another_below_baseline_pair(self) -> None:
        iceberg = self.document["iceberg_versions"][0]
        iceberg["upstream_minimums"]["3.5"] = "3.5.2"
        iceberg["spark_versions"][1]["comment"] = "Claimed compatibility exception"
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "only existing compatibility"):
            self._load_document(self.document)

    def test_packaged_family_cannot_be_hidden_by_removing_its_baseline(self) -> None:
        iceberg = self.document["iceberg_versions"][2]
        del iceberg["upstream_minimums"]["4.0"]
        iceberg["spark_versions"] = [row for row in iceberg["spark_versions"]
                                      if row["version"] != "4.0.1"]
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "missing 4.0.1"):
            self._load_document(self.document)

    def test_supported_family_requires_a_minimum(self) -> None:
        del self.document["iceberg_versions"][2]["upstream_minimums"]["4.0"]
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "missing minimum for Spark family"):
            self._load_document(self.document)

    def test_valid_requested_versions_preserve_caller_order(self) -> None:
        requested = ["1.10.1", "1.9.2"]
        self.assertEqual(
            requested,
            self.matrix.validate_requested_versions("3.5.6", requested))

    def test_unknown_requested_version_fails(self) -> None:
        with self.assertRaisesRegex(
                MATRIX_READER.MatrixError, "not present in the test matrix"):
            self.matrix.validate_requested_versions("3.5.6", ["9.9.9"])

    def test_requested_versions_without_matrix_entries_fail(self) -> None:
        for spark_version in ("3.5.0", "4.2.0"):
            with self.subTest(spark=spark_version):
                with self.assertRaisesRegex(MATRIX_READER.MatrixError, "no matrix entry"):
                    self.matrix.validate_requested_versions(spark_version, ["1.10.1"])

    def test_packaging_unsupported_requested_version_fails_with_reason(self) -> None:
        with self.assertRaisesRegex(
                MATRIX_READER.MatrixError, "Iceberg 1.6.x is not packaged"):
            self.matrix.validate_requested_versions("3.5.6", ["1.6.1"])

    def test_rejects_malformed_top_level_metadata(self) -> None:
        self.document["unexpected"] = True
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "must contain only"):
            self._load_document(self.document)

    def test_rejects_unsupported_entry_without_reason(self) -> None:
        spark = next(row for row in self.document["iceberg_versions"][0]["spark_versions"]
                     if not row["supported"])
        del spark["reason"]
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "needs a reason"):
            self._load_document(self.document)

    def test_rejects_missing_pom_derived_spark_mapping(self) -> None:
        entries = self.document["iceberg_versions"][0]["spark_versions"]
        entries[:] = [entry for entry in entries if entry["version"] != "3.5.1"]
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "missing 3.5.1"):
            self._load_document(self.document)

    def test_accepts_accurate_unsupported_records_for_declared_shims(self) -> None:
        # Below-baseline, other-family, and stub shims may all document non-support.
        for spark_version in ("3.5.0", "4.0.1", "4.2.0"):
            with self.subTest(spark=spark_version):
                document = copy.deepcopy(self.document)
                document["iceberg_versions"][1]["spark_versions"].append({
                    "version": spark_version, "supported": False,
                    "reason": "Iceberg 1.9.x is not packaged for this Spark shim",
                })
                matrix = self._load_document(document)
                self.assertEqual(self.matrix.supported_iceberg_versions(spark_version),
                                 matrix.supported_iceberg_versions(spark_version))
                with self.assertRaisesRegex(MATRIX_READER.MatrixError, "is not packaged"):
                    matrix.validate_requested_versions(spark_version, ["1.9.2"])

    def test_unsupported_records_are_optional(self) -> None:
        for iceberg in self.document["iceberg_versions"]:
            iceberg["spark_versions"] = [row for row in iceberg["spark_versions"]
                                         if row["supported"]]
        matrix = self._load_document(self.document)
        for spark_version in ("3.5.0", "3.5.1", "3.5.4", "3.5.7", "4.0.1", "4.2.0"):
            with self.subTest(spark=spark_version):
                self.assertEqual(self.matrix.supported_iceberg_versions(spark_version),
                                 matrix.supported_iceberg_versions(spark_version))

    def test_queries_do_not_invoke_maven_or_validate_other_mappings(self) -> None:
        self.document["iceberg_versions"][1]["spark_versions"].pop(0)
        self.matrix_path.write_text(json.dumps(self.document), encoding="utf-8")
        self.maven.reset_mock()
        self.maven.side_effect = FileNotFoundError("Maven must not run for queries")
        self.assertEqual((0, "1.9.2 1.10.1\n", ""),
                         self._run_cli("list", "--spark-version", "3.5.7"))
        self.assertEqual((0, "1.10.1\n", ""), self._run_cli(
            "validate-requested", "--spark-version", "3.5.7",
            "--requested-versions", "1.10.1"))
        self.maven.assert_not_called()

    def test_cli_explains_empty_selections(self) -> None:
        # 4.2.0 represents the stub family and genuinely has no packaged runtime.
        exit_code, stdout, stderr = self._run_cli("list", "--spark-version", "4.2.0")
        self.assertEqual(0, exit_code)
        self.assertEqual("\n", stdout)
        self.assertIn("No Iceberg versions selected for Spark 4.2.0", stderr)
        self.assertIn("matrix contains no enabled combination", stderr)

    def test_cli_requires_exact_spark_releases(self) -> None:
        for spark_version in ("3.5", "3.5.6-SNAPSHOT", "5.0.0-SNAPSHOT", "5.0.0-preview"):
            for command in ("list", "validate-requested"):
                with self.subTest(spark=spark_version, command=command):
                    arguments = ["--spark-version", spark_version]
                    if command == "validate-requested":
                        arguments += ["--requested-versions", "1.10.1"]
                    exit_code, stdout, stderr = self._run_cli(command, *arguments)
                    self.assertEqual(1, exit_code)
                    self.assertEqual("", stdout)
                    self.assertIn("invalid version", stderr)

    def test_cli_validates_requested_versions(self) -> None:
        exit_code, stdout, stderr = self._run_cli(
            "validate-requested", "--spark-version", "3.5.6",
            "--requested-versions", "1.10.1, 1.9.2")
        self.assertEqual(0, exit_code)
        self.assertEqual("1.10.1 1.9.2\n", stdout)
        self.assertEqual("", stderr)

    def test_cli_reports_malformed_json(self) -> None:
        self.matrix_path.write_text("{", encoding="utf-8")
        exit_code, stdout, stderr = self._run_cli("validate")
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("Iceberg test matrix error", stderr)

    def test_cli_validates_matrix_without_query_arguments(self) -> None:
        self.assertEqual((0, "", ""), self._run_cli("validate"))

    def test_cli_validate_fails_for_missing_pom_derived_mapping(self) -> None:
        entries = self.document["iceberg_versions"][1]["spark_versions"]
        entries[:] = [row for row in entries if row["version"] != "3.5.5"]
        self.matrix_path.write_text(json.dumps(self.document), encoding="utf-8")
        exit_code, stdout, stderr = self._run_cli("validate")
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("missing 3.5.5", stderr)

    def test_cli_validate_fails_for_malformed_pom(self) -> None:
        pom_path = self.matrix_path.parent / "malformed-pom.xml"
        pom_path.write_text("<project>", encoding="utf-8")
        exit_code, stdout, stderr = self._run_cli("validate", "--pom", str(pom_path))
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("Iceberg test matrix error", stderr)

    def test_cli_requested_version_failures_return_nonzero(self) -> None:
        cases = (
            ("3.5.6", "9.9.9", "not present in the test matrix"),
            ("3.5.6", "1.6.1", "Iceberg 1.6.x is not packaged"),
            ("3.5.0", "1.10.1", "no matrix entry"),
        )
        for spark_version, iceberg_version, diagnostic in cases:
            with self.subTest(spark=spark_version, iceberg=iceberg_version):
                exit_code, stdout, stderr = self._run_cli(
                    "validate-requested", "--spark-version", spark_version,
                    "--requested-versions", iceberg_version)
                self.assertEqual(1, exit_code)
                self.assertEqual("", stdout)
                self.assertIn(diagnostic, stderr)

    def test_cli_lists_versions_in_matrix_order(self) -> None:
        self.assertEqual(
            (0, "1.9.2 1.10.1\n", ""),
            self._run_cli("list", "--spark-version", "3.5.6"))

    def test_cli_requires_command_and_its_arguments(self) -> None:
        cases: Tuple[Tuple[List[str], str], ...] = (
            ([], "command"),
            (["list"], "--spark-version"),
            (["validate-requested", "--requested-versions", "1.10.1"], "--spark-version"),
            (["validate-requested", "--spark-version", "3.5.6"], "--requested-versions"),
        )
        for arguments, missing in cases:
            with self.subTest(arguments=arguments):
                stderr = io.StringIO()
                with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit) as error:
                    MATRIX_READER.parse_args(arguments)
                self.assertEqual(2, error.exception.code)
                self.assertIn("required", stderr.getvalue())
                self.assertIn(missing, stderr.getvalue())

    def test_cli_rejects_arguments_from_other_commands(self) -> None:
        cases = (
            ["validate", "--spark-version", "3.5.6"],
            ["validate", "--requested-versions", "1.10.1"],
            ["list", "--spark-version", "3.5.6", "--requested-versions", "1.10.1"],
            ["list", "--spark-version", "3.5.6", "--pom", "pom.xml"],
            ["validate-requested", "--spark-version", "3.5.6",
             "--requested-versions", "1.10.1", "--pom", "pom.xml"],
        )
        for arguments in cases:
            with self.subTest(arguments=arguments):
                stderr = io.StringIO()
                with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit) as error:
                    MATRIX_READER.parse_args(arguments)
                self.assertEqual(2, error.exception.code)
                self.assertIn("unrecognized arguments", stderr.getvalue())

    def test_json_decoder_constructs_nested_dataclasses(self) -> None:
        self.assertIsInstance(self.matrix, MATRIX_READER.IcebergVersionMatrix)
        iceberg_support = self.matrix.iceberg_supports[MATRIX_READER.IcebergVersion.parse("1.6.1")]
        self.assertIsInstance(iceberg_support, MATRIX_READER.IcebergSupport)
        spark_support = iceberg_support.spark_supports[MATRIX_READER.SparkVersion.parse("3.5.1")]
        self.assertIsInstance(spark_support, MATRIX_READER.SparkSupport)
        self.assertIsNone(spark_support.reason)
        self.assertEqual({"3.5": MATRIX_READER.SparkVersion.parse("3.5.1")},
                         iceberg_support.spark_minor_to_patch)

    def test_numeric_version_ordering_and_distinct_domains(self) -> None:
        self.assertGreater(MATRIX_READER.SparkVersion.parse("3.5.10"),
                           MATRIX_READER.SparkVersion.parse("3.5.9"))
        self.assertGreater(MATRIX_READER.IcebergVersion.parse("1.10.1"),
                           MATRIX_READER.IcebergVersion.parse("1.9.2"))
        self.assertNotEqual(MATRIX_READER.SparkVersion.parse("1.2.3"),
                            MATRIX_READER.IcebergVersion.parse("1.2.3"))

    def test_version_construction_requires_nonnegative_integer_components(self) -> None:
        for version_type in (MATRIX_READER.SparkVersion, MATRIX_READER.IcebergVersion):
            for part in (-1, True, 1.5, "1"):
                for index in range(3):
                    parts: List[Any] = [1, 2, 3]
                    parts[index] = part
                    with self.subTest(kind=version_type, parts=parts):
                        with self.assertRaisesRegex(
                                MATRIX_READER.MatrixError, "invalid release components"):
                            version_type(*parts)

    def test_version_parsers_reject_non_release_strings(self) -> None:
        for version_type in (MATRIX_READER.SparkVersion, MATRIX_READER.IcebergVersion):
            for version in ("3.5", "3.5.7-SNAPSHOT", "3.5.7-preview", "-1.2.3"):
                with self.subTest(kind=version_type, version=version):
                    with self.assertRaisesRegex(MATRIX_READER.MatrixError, "invalid version"):
                        version_type.parse(version)

    def test_version_keyed_lookups_preserve_decoded_records(self) -> None:
        iceberg = self.matrix.iceberg_supports[MATRIX_READER.IcebergVersion.parse("1.9.2")]
        spark_version = MATRIX_READER.SparkVersion.parse("3.5.7")
        self.assertIs(iceberg.spark_supports[spark_version], iceberg.support_for(spark_version))
        self.assertIsNone(iceberg.support_for(MATRIX_READER.SparkVersion.parse("3.5.0")))

    def test_rejects_duplicate_versions_before_indexing(self) -> None:
        for kind in ("Iceberg", "Spark"):
            with self.subTest(kind=kind):
                document = copy.deepcopy(self.document)
                entries = document["iceberg_versions"]
                if kind == "Spark":
                    entries = entries[0]["spark_versions"]
                entries.append(copy.deepcopy(entries[0]))
                with self.assertRaisesRegex(MATRIX_READER.MatrixError, f"duplicate {kind} version"):
                    self._load_document(document)

    def test_dictionary_selection_preserves_json_order(self) -> None:
        self.document["iceberg_versions"].reverse()
        matrix = self._load_document(self.document)
        self.assertEqual(["1.10.1", "1.9.2"], matrix.supported_iceberg_versions("3.5.7"))
        requested = ["1.9.2", "1.10.1", "1.9.2"]
        self.assertEqual(requested, matrix.validate_requested_versions("3.5.7", requested))

    def test_infers_spark357_runtime_combinations_from_module_poms(self) -> None:
        packaging = MATRIX_READER.MavenPackaging.load(self.pom_path)
        self.assertEqual({MATRIX_READER.IcebergVersion.parse("1.9.2"),
                          MATRIX_READER.IcebergVersion.parse("1.10.1")},
                         packaging.iceberg_versions_by_spark[MATRIX_READER.SparkVersion.parse("3.5.7")])
        self.assertEqual(["1.9.2", "1.10.1"], self.matrix.supported_iceberg_versions("3.5.7"))
        self.assertTrue(packaging.supports(MATRIX_READER.SparkVersion.parse("3.5.4"),
                                           MATRIX_READER.IcebergVersion.parse("1.9.2")))
        self.assertEqual(["1.9.2", "1.10.1"], self.matrix.supported_iceberg_versions("3.5.4"))
        self.assertEqual(set(), packaging.iceberg_versions_by_spark[
            MATRIX_READER.SparkVersion.parse("4.2.0")])

    def test_rejects_support_flags_that_disagree_with_maven_packaging(self) -> None:
        for iceberg_version, supported in (("1.9.2", False), ("1.6.1", True)):
            with self.subTest(iceberg=iceberg_version, supported=supported):
                document = copy.deepcopy(self.document)
                iceberg_support = next(entry for entry in document["iceberg_versions"]
                                       if entry["version"] == iceberg_version)
                spark_support = next(entry for entry in iceberg_support["spark_versions"]
                                     if entry["version"] == "3.5.7")
                spark_support["supported"] = supported
                if supported:
                    del spark_support["reason"]
                else:
                    spark_support["reason"] = "Incorrect fixture flag"
                diagnostic = "unexpected 3.5.7" if supported else "missing 3.5.7"
                with self.assertRaisesRegex(MATRIX_READER.MatrixError, diagnostic):
                    self._load_document(document)

    def test_module_removal_fails_even_when_spark_shims_are_unchanged(self) -> None:
        tree = ET.parse(self.pom_path)
        namespace = MATRIX_READER.POM_NAMESPACE
        profile = tree.getroot().find("pom:profiles/pom:profile[pom:id='release357']", namespace)
        assert profile is not None
        modules = profile.find("pom:modules", namespace)
        assert modules is not None
        modules.remove(next(module for module in modules
                            if module.text == "iceberg/iceberg-1-10-x"))
        tree.write(self.pom_path, encoding="utf-8", xml_declaration=True)
        profile_path = self.pom_path.parent / "effective-release357.xml"
        tree = ET.parse(profile_path)
        tree.getroot().remove(tree.getroot()[-1])
        tree.write(profile_path, encoding="utf-8", xml_declaration=True)
        exit_code, stdout, stderr = self._run_cli("validate")
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("Iceberg 1.10.1 Spark support disagrees with Maven packaging", stderr)
        self.assertIn("unexpected 3.5.7", stderr)
        self.assertIn(
            ((self.pom_path, "release357", ["iceberg/iceberg-1-9-x"]), {}),
            self.maven.call_args_list)

    def test_rejects_packaged_runtime_missing_from_matrix(self) -> None:
        self.document["iceberg_versions"].pop(1)
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "missing from the matrix: 1.9.2"):
            self._load_document(self.document)

    def test_rejects_matrix_runtime_not_packaged_by_maven(self) -> None:
        self.document["iceberg_versions"].append({
            "version": "9.9.9", "upstream_minimums": {"3.5": "3.5.0"},
            "spark_versions": [{"version": "3.5.0", "supported": False,
                                "reason": "Not packaged"}],
        })
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "not packaged by Maven: 9.9.9"):
            self._load_document(self.document)

    def test_rejects_matrix_spark_version_without_pom_shim(self) -> None:
        for supported in (True, False):
            with self.subTest(supported=supported):
                document = copy.deepcopy(self.document)
                entry = {"version": "3.5.8", "supported": supported}
                if not supported:
                    entry["reason"] = "Claimed unsupported shim"
                document["iceberg_versions"][1]["spark_versions"].append(entry)
                with self.assertRaisesRegex(MATRIX_READER.MatrixError, "unknown Spark shims: 3.5.8"):
                    self._load_document(document)

    def test_effective_runtime_pin_is_used_instead_of_module_name(self) -> None:
        self._change_effective_runtime("1.9.3")
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "missing from the matrix: 1.9.3"):
            self._load_document(self.document)

    def test_maven_effective_dependency_overrides_parent_runtime_pin(self) -> None:
        self._change_effective_runtime("1.9.3")
        self.document["iceberg_versions"][1]["version"] = "1.9.3"
        self.document["iceberg_versions"][1]["upstream_minimums"]["3.5"] = "3.5.4"
        matrix = self._load_document(self.document)
        self.assertEqual(["1.9.3", "1.10.1"], matrix.supported_iceberg_versions("3.5.7"))

    def _change_effective_runtime(self, version: str) -> None:
        for path in self.pom_path.parent.glob("effective-release*.xml"):
            tree = ET.parse(path)
            for dependency in tree.getroot().findall(
                    "pom:project/pom:dependencies/pom:dependency", MATRIX_READER.POM_NAMESPACE):
                pin = dependency.find("pom:version", MATRIX_READER.POM_NAMESPACE)
                assert pin is not None
                if pin.text == "1.9.2":
                    pin.text = version
            tree.write(path, encoding="utf-8", xml_declaration=True)

    def test_cli_reports_maven_failure_without_skipping(self) -> None:
        self.maven.side_effect = MATRIX_READER.MatrixError("Maven effective-POM failed: bad model")
        exit_code, stdout, stderr = self._run_cli("validate")
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("Maven effective-POM failed: bad model", stderr)

    def test_cli_reports_missing_maven(self) -> None:
        self.maven.side_effect = FileNotFoundError("mvn is unavailable")
        exit_code, stdout, stderr = self._run_cli("validate")
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("mvn is unavailable", stderr)

    def test_json_decoder_rejects_unknown_record_fields(self) -> None:
        for record in ("iceberg", "spark"):
            with self.subTest(record=record):
                document = copy.deepcopy(self.document)
                entry = document["iceberg_versions"][0]
                if record == "spark":
                    entry = entry["spark_versions"][0]
                entry["unexpected"] = True
                with self.assertRaises(MATRIX_READER.MatrixError):
                    self._load_document(document)

    def test_json_annotations_reject_incorrect_scalar_types(self) -> None:
        cases: Tuple[Tuple[str, Any, str], ...] = (
            ("version", 351, "invalid version"),
            ("version", None, "invalid version"),
            ("supported", "false", "expected bool"),
            ("supported", 1, "expected bool"),
            ("supported", None, "expected bool"),
            ("reason", False, "expected str"),
            ("reason", [], "expected str"),
            ("comment", False, "expected str"),
            ("comment", [], "expected str"),
        )
        for name, value, diagnostic in cases:
            with self.subTest(field=name, value=value):
                document = copy.deepcopy(self.document)
                document["iceberg_versions"][0]["spark_versions"][0][name] = value
                with self.assertRaisesRegex(MATRIX_READER.MatrixError, diagnostic):
                    self._load_document(document)

    def test_json_annotations_require_nonempty_collections_of_the_declared_type(self) -> None:
        cases: Tuple[Tuple[str, Any], ...] = (
            ("iceberg_versions", []),
            ("iceberg_versions", {}),
            ("iceberg_versions", ["1.6.1"]),
            ("spark_versions", []),
            ("spark_versions", {}),
            ("spark_versions", ["3.5.1"]),
            ("spark_versions", [self.document["iceberg_versions"][0]]),
            ("upstream_minimums", {}),
            ("upstream_minimums", []),
            ("upstream_minimums", {"3.5": 351}),
        )
        for name, value in cases:
            with self.subTest(field=name, value=value):
                document = copy.deepcopy(self.document)
                record = document if name == "iceberg_versions" else document["iceberg_versions"][0]
                record[name] = value
                with self.assertRaises(MATRIX_READER.MatrixError):
                    self._load_document(document)

    def test_json_records_enforce_cross_field_rules(self) -> None:
        for supported, reason in ((True, "excluded"), (False, None), (False, "  ")):
            with self.subTest(supported=supported, reason=reason):
                document = copy.deepcopy(self.document)
                document["iceberg_versions"][0]["spark_versions"][0].update(
                    supported=supported, reason=reason)
                with self.assertRaisesRegex(MATRIX_READER.MatrixError, "reason"):
                    self._load_document(document)
        self.document["iceberg_versions"][0]["upstream_minimums"] = {"3.5": "4.0.0"}
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "does not belong to Spark family"):
            self._load_document(self.document)

    def test_rejects_release_profile_without_declared_spark_shim(self) -> None:
        tree = ET.parse(self.pom_path)
        namespace = MATRIX_READER.POM_NAMESPACE
        version = tree.getroot().find(
            "pom:profiles/pom:profile[pom:id='release357']/pom:properties/pom:spark.version",
            namespace)
        assert version is not None
        version.text = "3.5.9"
        tree.write(self.pom_path, encoding="utf-8", xml_declaration=True)
        profile_path = self.pom_path.parent / "effective-release357.xml"
        tree = ET.parse(profile_path)
        version = tree.getroot().find("pom:project/pom:properties/pom:spark.version", namespace)
        assert version is not None
        version.text = "3.5.9"
        tree.write(profile_path, encoding="utf-8", xml_declaration=True)
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "3.5.9 has no declared shim property"):
            self._load_document(self.document)


if __name__ == "__main__":
    unittest.main()

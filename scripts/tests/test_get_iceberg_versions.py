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
import os
import subprocess
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
        return MATRIX_READER.IcebergVersionMatrix.load(self.matrix_path, self.pom_path)

    def _run_cli(self, command: str, *arguments: str) -> Tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = MATRIX_READER.main([
                command, "--matrix", str(self.matrix_path),
                "--pom", str(self.pom_path), *arguments])
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

    def test_minimum_baselines_intentionally_exclude_older_patches(self) -> None:
        for spark_version in ("3.5.0", "3.5.4"):
            with self.subTest(spark=spark_version):
                self.assertEqual([], self.matrix.supported_iceberg_versions(spark_version))
        self.assertEqual(["1.9.2"], self.matrix.supported_iceberg_versions("3.5.5"))

    def test_valid_requested_versions_preserve_caller_order(self) -> None:
        requested = ["1.10.1", "1.9.2"]
        self.assertEqual(
            requested,
            self.matrix.validate_requested_versions("3.5.6", requested))

    def test_unknown_requested_version_fails(self) -> None:
        with self.assertRaisesRegex(
                MATRIX_READER.MatrixError, "not present in the test matrix"):
            self.matrix.validate_requested_versions("3.5.6", ["9.9.9"])

    def test_requested_versions_outside_baseline_policy_fail(self) -> None:
        for spark_version in ("3.5.5", "4.2.0"):
            with self.subTest(spark=spark_version):
                with self.assertRaisesRegex(MATRIX_READER.MatrixError, "minimum-baseline"):
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
        del self.document["iceberg_versions"][0]["spark_versions"][1]["reason"]
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "needs a reason"):
            self._load_document(self.document)

    def test_rejects_missing_pom_derived_spark_mapping(self) -> None:
        entries = self.document["iceberg_versions"][0]["spark_versions"]
        entries[:] = [entry for entry in entries if entry["version"] != "3.5.6"]
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "missing 3.5.6"):
            self._load_document(self.document)

    def test_cli_fails_for_missing_mapping_instead_of_skipping(self) -> None:
        self.document["iceberg_versions"][1]["spark_versions"].pop(0)
        self.matrix_path.write_text(json.dumps(self.document), encoding="utf-8")
        exit_code, stdout, stderr = self._run_cli("list", "--spark-version", "3.5.5")
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("missing 3.5.5", stderr)

    def test_cli_explains_empty_selections(self) -> None:
        # 4.2.0 represents the stub family. Below-baseline patches are also valid
        # empty selections under issue #15875, even if their POM packages Iceberg.
        for spark_version in ("4.2.0", "3.5.0", "3.5.4"):
            with self.subTest(spark=spark_version):
                exit_code, stdout, stderr = self._run_cli("list", "--spark-version", spark_version)
                self.assertEqual(0, exit_code)
                self.assertEqual("\n", stdout)
                self.assertIn(f"No Iceberg versions selected for Spark {spark_version}", stderr)
                self.assertIn("minimum-baseline and packaging test policy", stderr)

    def test_cli_skips_snapshot_without_iceberg_family(self) -> None:
        exit_code, stdout, stderr = self._run_cli("list", "--spark-version", "5.0.0-SNAPSHOT")
        self.assertEqual(0, exit_code)
        self.assertEqual("\n", stdout)
        self.assertIn("No Iceberg versions selected for Spark 5.0.0-SNAPSHOT", stderr)

    def test_cli_rejects_snapshot_in_matrix_family(self) -> None:
        # Do not silently skip or select the corresponding release for a family
        # whose Iceberg coverage is defined by the matrix.
        exit_code, stdout, stderr = self._run_cli("list", "--spark-version", "3.5.6-SNAPSHOT")
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("invalid version", stderr)

    def test_cli_rejects_malformed_snapshot_versions(self) -> None:
        for spark_version in ("5.0-SNAPSHOT", "5.0.0-SNAPSHOT-SNAPSHOT", "5.0.0-preview"):
            with self.subTest(spark=spark_version):
                exit_code, stdout, stderr = self._run_cli("list", "--spark-version", spark_version)
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
        self.document["iceberg_versions"][1]["spark_versions"].pop(0)
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
            ("3.5.5", "1.10.1", "minimum-baseline"),
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
        # Maven packaging still does not admit patches below the test baselines.
        self.assertTrue(packaging.supports(MATRIX_READER.SparkVersion.parse("3.5.4"),
                                           MATRIX_READER.IcebergVersion.parse("1.9.2")))
        self.assertEqual([], self.matrix.supported_iceberg_versions("3.5.4"))
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
                with self.assertRaisesRegex(MATRIX_READER.MatrixError, "Maven packaging"):
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
        self.assertIn("Iceberg 1.10.1, Spark 3.5.7", stderr)
        self.assertIn("Maven packaging (expected False)", stderr)
        self.assertIn(
            ((self.pom_path, "release357", ["iceberg/iceberg-1-9-x"]), {}),
            self.maven.call_args_list)

    def test_rejects_packaged_runtime_missing_from_matrix(self) -> None:
        self.document["iceberg_versions"].pop(1)
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "missing from the matrix: 1.9.2"):
            self._load_document(self.document)

    def test_effective_runtime_pin_is_used_instead_of_module_name(self) -> None:
        self._change_effective_runtime("1.9.3")
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "missing from the matrix: 1.9.3"):
            self._load_document(self.document)

    def test_maven_effective_dependency_overrides_parent_runtime_pin(self) -> None:
        self._change_effective_runtime("1.9.3")
        self.document["iceberg_versions"][1]["version"] = "1.9.3"
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
        exit_code, stdout, stderr = self._run_cli("list", "--spark-version", "3.5.7")
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


class MavenEffectivePomSuite(unittest.TestCase):
    def _run_maven(self, command: List[str], **arguments: Any) -> subprocess.CompletedProcess[str]:
        output_argument = next(value for value in command if value.startswith("-Doutput="))
        output = Path(output_argument.split("=", 1)[1])
        fixture = "effective-release357.xml" if "-P" in command else "effective-parent.xml"
        output.write_text((FIXTURE_DIR / fixture).read_text(), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="Maven logging is captured\n")

    def test_evaluates_selected_profile_modules_with_existing_maven_settings(self) -> None:
        with patch.dict(os.environ, {"MVN": 'mvn -s "settings with spaces.xml"'}), \
                patch.object(subprocess, "run", side_effect=self._run_maven) as run:
            projects = MATRIX_READER._effective_pom(
                Path("parent/pom.xml"), "release357",
                ["iceberg/iceberg-1-9-x", "iceberg/iceberg-1-10-x"])
        command = run.call_args[0][0]
        self.assertEqual(["mvn", "-s", "settings with spaces.xml"], command[:3])
        self.assertEqual(str(Path("parent/pom.xml").resolve()), command[command.index("-f") + 1])
        self.assertEqual("release357", command[command.index("-P") + 1])
        self.assertEqual(".,iceberg/iceberg-1-9-x,iceberg/iceberg-1-10-x",
                         command[command.index("-pl") + 1])
        self.assertIn("help:effective-pom", command)
        self.assertTrue(run.call_args[1]["check"])
        self.assertEqual(subprocess.PIPE, run.call_args[1]["stdout"])
        self.assertEqual(subprocess.STDOUT, run.call_args[1]["stderr"])
        self.assertEqual(3, len(projects))

    def test_parent_discovery_is_non_recursive(self) -> None:
        with patch.object(subprocess, "run", side_effect=self._run_maven) as run:
            projects = MATRIX_READER._effective_pom(Path("parent/pom.xml"))
        self.assertEqual(1, len(projects))
        self.assertIn("-N", run.call_args[0][0])
        self.assertNotIn("-pl", run.call_args[0][0])

    def test_maven_failure_includes_its_diagnostic(self) -> None:
        error = subprocess.CalledProcessError(1, ["mvn"], output="cyclic model property")
        with patch.object(subprocess, "run", side_effect=error):
            with self.assertRaisesRegex(MATRIX_READER.MatrixError, "cyclic model property"):
                MATRIX_READER._effective_pom(Path("pom.xml"), "release357")


class IcebergCatalogCallerSuite(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.workspace = Path(directory.name) / "workspace with spaces"
        self.artifacts = self.workspace / "jars" / "integration_tests"
        self.artifacts.mkdir(parents=True)
        scripts = self.workspace / "scripts"
        scripts.mkdir()
        (scripts / "get_iceberg_versions.py").write_text(
            (REPO_ROOT / "scripts" / "get_iceberg_versions.py").read_text(), encoding="utf-8")
        (scripts / "iceberg-versions.json").write_text(
            (FIXTURE_DIR / "iceberg-versions.json").read_text(), encoding="utf-8")
        pom = self.workspace / "scala2.13" / "pom.xml"
        pom.parent.mkdir()
        pom.write_text((FIXTURE_DIR / "effective-parent.xml").read_text(), encoding="utf-8")
        settings = self.workspace / "jenkins" / "settings.xml"
        settings.parent.mkdir()
        settings.write_text("<settings />", encoding="utf-8")
        binary_dir = self.workspace / "bin"
        binary_dir.mkdir()
        python = binary_dir / "python"
        python.write_text(
            '#!/usr/bin/env bash\nprintf "%s\\n" "$2" >> "$READER_LOG"\n'
            'exec "$PYTHON_EXECUTABLE" "$@"\n', encoding="utf-8")
        python.chmod(0o755)
        self.invocations = self.workspace / "runner.log"
        self.maven_log = self.workspace / "maven.log"
        self.cwd_log = self.workspace / "cwd.log"
        self.reader_log = self.workspace / "reader.log"
        self.marker_log = self.workspace / "markers.log"
        runner = self.artifacts / "run_pyspark_from_build.sh"
        runner.write_text(
            '#!/usr/bin/env bash\nprintf "%s|%s\\n" "$EXPECTED_ICEBERG_VERSION" "$PWD" '
            '>> "$INVOCATIONS"\n'
            'while [[ $# -gt 0 ]]; do\n'
            '  if [[ "$1" == "-m" ]]; then\n'
            '    printf "%s\\n" "$2" >> "$MARKER_LOG"\n'
            '    break\n'
            '  fi\n'
            '  shift\n'
            'done\n', encoding="utf-8")
        runner.chmod(0o755)
        source = (REPO_ROOT / "jenkins" / "spark-tests.sh").read_text()
        start = source.index("run_iceberg_tests() {")
        end = source.index("\n}\n", start) + len("\n}\n")
        self.definition = self.workspace / "functions.sh"
        self.definition.write_text(source[start:end], encoding="utf-8")

    def _run_catalog(self, catalog: str, requested: Optional[str] = None,
                     settings: str = "jenkins/settings.xml", spark_version: str = "3.5.7",
                     test_tags: Optional[str] = None) -> Tuple[
                         subprocess.CompletedProcess[str], List[str], List[str]]:
        for log in (self.invocations, self.maven_log, self.cwd_log, self.reader_log, self.marker_log):
            if log.exists():
                log.unlink()
        environment = os.environ.copy()
        for name in ("MVN", "ICEBERG_VERSIONS", "TEST_TAGS"):
            environment.pop(name, None)
        environment.update(
            WORKSPACE=str(self.workspace), SPARK_VER=spark_version, SCALA_BINARY_VER="2.13",
            PROJECT_REPO_HOST="unused", ICEBERG_REST_CREDENTIAL="unused",
            MAVEN_FIXTURES=str(FIXTURE_DIR), FAKE_MAVEN=str(FIXTURE_DIR / "fake-maven.sh"),
            MAVEN_LOG=str(self.maven_log), INVOCATIONS=str(self.invocations),
            CWD_LOG=str(self.cwd_log), DEFINITION=str(self.definition),
            READER_LOG=str(self.reader_log), PYTHON_EXECUTABLE=sys.executable,
            MARKER_LOG=str(self.marker_log),
            MVN_SETTINGS=settings, CATALOG=catalog,
            PATH=str(self.workspace / "bin") + os.pathsep + environment.get("PATH", ""))
        if requested is not None:
            environment["ICEBERG_VERSIONS"] = requested
        if test_tags is not None:
            environment["TEST_TAGS"] = test_tags
        command = '''set -e
source "$DEFINITION"
download_maven_jars() { printf '/fixture-jars\\n'; }
MVN="bash \\"$FAKE_MAVEN\\" -s \\"$MVN_SETTINGS\\""
run_iceberg_tests "$CATALOG"
printf '%s\\n' "$PWD" > "$CWD_LOG"
'''
        result = subprocess.run(["bash", "-c", command], cwd=self.artifacts,
                                env=environment, text=True, capture_output=True)
        calls = self.invocations.read_text().splitlines() if self.invocations.exists() else []
        maven_calls = self.maven_log.read_text().splitlines() if self.maven_log.exists() else []
        return result, calls, maven_calls

    def test_relative_settings_for_all_catalogs(self) -> None:
        for catalog in ("default", "rest", "s3tables"):
            with self.subTest(catalog=catalog):
                result, calls, maven_calls = self._run_catalog(catalog)
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                self.assertEqual([f"1.9.2|{self.artifacts}", f"1.10.1|{self.artifacts}"], calls)
                self.assertEqual([f"{self.workspace}|jenkins/settings.xml"] * 9, maven_calls)
                self.assertEqual(["list"], self.reader_log.read_text().splitlines())
                self.assertEqual(["iceberg"] * 2, self.marker_log.read_text().splitlines())
                self.assertEqual(str(self.artifacts), self.cwd_log.read_text().strip())

    def test_requested_versions_use_workspace_settings_for_all_catalogs(self) -> None:
        for catalog in ("default", "rest", "s3tables"):
            with self.subTest(catalog=catalog):
                result, calls, maven_calls = self._run_catalog(catalog, "1.10.1, 1.9.2")
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                self.assertEqual([f"1.10.1|{self.artifacts}", f"1.9.2|{self.artifacts}"], calls)
                self.assertEqual([f"{self.workspace}|jenkins/settings.xml"] * 9, maven_calls)
                self.assertEqual(["validate-requested"], self.reader_log.read_text().splitlines())
                self.assertEqual(str(self.artifacts), self.cwd_log.read_text().strip())

    def test_missing_settings_fail_before_running_any_catalog_tests(self) -> None:
        for catalog in ("default", "rest", "s3tables"):
            with self.subTest(catalog=catalog):
                result, calls, maven_calls = self._run_catalog(catalog, settings="missing.xml")
                self.assertNotEqual(0, result.returncode)
                self.assertEqual([], calls)
                self.assertEqual([f"{self.workspace}|missing.xml"], maven_calls)
                self.assertIn("missing Maven settings: missing.xml", result.stderr)


    def test_marker_selector_reaches_all_catalog_runners(self) -> None:
        for catalog in ("default", "rest", "s3tables"):
            with self.subTest(catalog=catalog):
                result, calls, _ = self._run_catalog(catalog, test_tags="iceberg_fast_test")
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                self.assertEqual(2, len(calls))
                self.assertEqual(["iceberg and (iceberg_fast_test)"] * 2,
                                 self.marker_log.read_text().splitlines())

    def test_empty_selections_warn_for_all_catalogs(self) -> None:
        for catalog in ("default", "rest", "s3tables"):
            for requested in (None, " , "):
                with self.subTest(catalog=catalog, requested=requested):
                    result, calls, _ = self._run_catalog(
                        catalog, requested=requested, spark_version="3.5.4")
                    self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                    self.assertEqual([], calls)
                    self.assertIn("No supported Iceberg version found for Spark 3.5.4", result.stdout)
                    self.assertEqual(["list" if requested is None else "validate-requested"],
                                     self.reader_log.read_text().splitlines())

    def test_invalid_requests_fail_without_listing_or_starting_tests(self) -> None:
        for catalog in ("default", "rest", "s3tables"):
            with self.subTest(catalog=catalog):
                result, calls, _ = self._run_catalog(catalog, requested="9.9.9")
                self.assertNotEqual(0, result.returncode)
                self.assertEqual([], calls)
                self.assertEqual(["validate-requested"], self.reader_log.read_text().splitlines())
                self.assertIn("not present in the test matrix", result.stderr)
                self.assertNotIn("No supported Iceberg version found", result.stdout)


if __name__ == "__main__":
    unittest.main()

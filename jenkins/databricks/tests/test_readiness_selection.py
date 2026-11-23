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

"""Exercise Databricks CI test selection without installing or starting Spark."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class ReadinessSelectionTest(unittest.TestCase):
    def run_script(self, version="18.3.", mode="CI_PART1", hook=True,
                   override=None, extra_envs="", upstream="1", hook_exit=0, delta_exit=0,
                   jar_path="", delta_report="<testcase name='test_delta_scan_read'/>",
                   unsafe_delta_directory=False):
        script = Path(__file__).resolve().parents[1] / "test.sh"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_root = root / jar_path if jar_path else root
            db_dir = run_root / "jenkins/databricks"
            db_dir.mkdir(parents=True)
            (db_dir / "setup.sh").write_text(":\n")
            (db_dir / "common_vars.sh").write_text(":\n")
            integration_dir = run_root / "integration_tests"
            integration_dir.mkdir()
            (integration_dir / "run_pyspark_from_build.sh").write_text(
                'echo "PRODUCT_TEST $*"\necho "TEST_FILES ${TESTS:-}"\n'
                'if [[ " $* " == *" --delta_lake "* ]]; then\n'
                f'  if [[ {delta_exit} != 0 ]]; then exit {delta_exit}; fi\n'
                '  if [[ -n "$LOCAL_JAR_PATH" ]]; then\n'
                '    [[ "$LOCAL_JAR_PATH" == "$PWD" && "$DRIVER_MEMORY" == 4g &&\n'
                '       "$RUN_DIR" == "$PWD"/integration_tests/target/run_dir-* ]] || exit 2\n'
                '    echo "DELTA_RUN_DIR $RUN_DIR"\n'
                '    if [[ -n "$DELTA_REPORT" ]]; then\n'
                '      printf "<testsuite>%s</testsuite>\\n" "$DELTA_REPORT" '
                '> "$RUN_DIR/TEST-pytest-delta.xml"\n'
                '    fi\n'
                '  fi\n'
                'fi\n')
            if jar_path:
                # A passing report from another pass must not satisfy the Delta pass.
                other_run = integration_dir / "target/run_dir-other"
                other_run.mkdir(parents=True)
                (other_run / "TEST-pytest-other.xml").write_text(
                    '<testsuite><testcase name="test_unrelated"/></testsuite>\n')
            startup = root / "startup.sh"
            startup.write_text(f'echo "READINESS $1"\nexit {hook_exit}\n')
            # Keep the actual control flow, with external setup/Spark calls stubbed.
            (root / "test.sh").write_text(script.read_text().replace(
                'SOURCE_PATH="/home/ubuntu/spark-rapids"', f'SOURCE_PATH="{root}"'))
            (root / "spark-3.3.0-bin-hadoop3").mkdir()
            env = {key: value for key, value in os.environ.items()
                   if key in {"PATH", "LANG"}}
            env.update(HOME=str(root), LOCAL_JAR_PATH=jar_path, DELTA_REPORT=delta_report,
                       BASE_SPARK_VERSION="3.3.0", TEST_MODE=mode,
                       EXTRA_ENVS=extra_envs, WITH_DEFAULT_UPSTREAM_SHIM=upstream,
                       TEST_TYPE="nightly", PYSPARK_PYTHON=sys.executable,
                       PYSP_TEST_spark_databricks_clusterUsageTags_sparkVersion=version)
            if unsafe_delta_directory:
                mock_bin = root / "bin"
                mock_bin.mkdir()
                mktemp = mock_bin / "mktemp"
                mktemp.write_text(
                    '#!/bin/bash\n'
                    'if [[ ! -f "$HOME/unsafe_name_attempted" ]]; then\n'
                    '    touch "$HOME/unsafe_name_attempted"\n'
                    '    dir=${2%XXXXXX}XmxABC\n'
                    'else\n'
                    '    [[ ! -e "${2%XXXXXX}XmxABC" ]] || exit 1\n'
                    '    dir=${2%XXXXXX}abcdef\n'
                    'fi\n'
                    'mkdir "$dir" && printf "%s\\n" "$dir"\n')
                mktemp.chmod(0o755)
                env["PATH"] = str(mock_bin) + os.pathsep + env["PATH"]
            if hook:
                env["SPARK_STARTUP_CHECK"] = str(startup)
            if override is not None:
                env["SPARK_READINESS_ONLY"] = override
            return subprocess.run(["bash", str(root / "test.sh")], env=env,
                                  cwd=root, text=True, capture_output=True, check=False)

    def test_ci_runs_integration_tests_for_dbr_183(self):
        for mode in ("DEFAULT", "CI_PART1", "CI_PART2"):
            with self.subTest(mode=mode):
                result = self.run_script(mode=mode)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("READINESS databricks", result.stdout)
                self.assertEqual("READINESS upstream" in result.stdout, mode != "CI_PART2")
                self.assertIn("PRODUCT_TEST", result.stdout)
                if mode == "CI_PART1":
                    self.assertNotIn("--delta_lake", result.stdout)
                else:
                    self.assertIn("-m delta_lake --delta_lake", result.stdout)

    def test_manual_and_other_runtimes_keep_integration_tests(self):
        for version, hook in (("18.3.", False), ("17.3.", True),
                              ("18.30.", True), ("19.3.", True)):
            for mode in ("DEFAULT", "CI_PART1", "CI_PART2"):
                with self.subTest(version=version, hook=hook, mode=mode):
                    result = self.run_script(version=version, hook=hook, mode=mode)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("PRODUCT_TEST", result.stdout)
                    self.assertEqual("READINESS databricks" in result.stdout, hook)

    def test_explicit_zero_enables_integration_tests(self):
        for mode in ("DEFAULT", "CI_PART1", "CI_PART2"):
            with self.subTest(mode=mode):
                result = self.run_script(mode=mode, override="0")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("READINESS databricks", result.stdout)
                self.assertIn("PRODUCT_TEST", result.stdout)
        result = self.run_script(extra_envs="SPARK_READINESS_ONLY=0")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("PRODUCT_TEST", result.stdout)

    def test_disabled_upstream_shim_only_checks_native_spark(self):
        result = self.run_script(upstream="0")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("READINESS databricks", result.stdout)
        self.assertNotIn("READINESS upstream", result.stdout)
        self.assertIn("PRODUCT_TEST", result.stdout)

    def test_ci_part2_includes_files_matched_by_part1_substrings(self):
        result = self.run_script(mode="CI_PART2")
        self.assertEqual(result.returncode, 0, result.stderr)
        files = next(line.removeprefix("TEST_FILES ").split()
                     for line in result.stdout.splitlines()
                     if line.startswith("TEST_FILES ") and line != "TEST_FILES ")
        self.assertEqual(set(files), {"json_test.py", "get_json_test.py", "orc_test.py",
                                      "parquet_test.py", "parquet_testing_test.py",
                                      "sort_test.py", "window_function_test.py"})

    def test_other_runtimes_and_manual_runs_keep_delta_lake(self):
        for version, hook in (("14.3.", True), ("17.3.", True), ("18.3.", False)):
            with self.subTest(version=version, hook=hook):
                result = self.run_script(version=version, hook=hook, mode="CI_PART2")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("--delta_lake", result.stdout)

    def test_explicit_delta_only_mode_is_preserved(self):
        result = self.run_script(mode="DELTA_LAKE_ONLY")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("-m delta_lake --delta_lake", result.stdout)

    def test_explicit_readiness_requires_hook_and_supported_mode(self):
        result = self.run_script(hook=False, override="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires the CI Spark readiness hook", result.stderr)
        result = self.run_script(mode="DELTA_LAKE_ONLY", override="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not support TEST_MODE=DELTA_LAKE_ONLY", result.stderr)

    def test_explicit_readiness_override_is_preserved_for_other_runtimes(self):
        result = self.run_script(version="17.3.", override="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("READINESS databricks", result.stdout)
        self.assertNotIn("PRODUCT_TEST", result.stdout)

    def test_failed_readiness_still_fails_ci(self):
        result = self.run_script(hook_exit=9)
        self.assertEqual(result.returncode, 9, result.stderr)
        self.assertNotIn("PRODUCT_TEST", result.stdout)

    def test_failed_delta_suite_stops_ci_part2(self):
        result = self.run_script(mode="CI_PART2", delta_exit=7)
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertIn("-m delta_lake --delta_lake", result.stdout)
        self.assertNotIn("-m shuffle_test", result.stdout)
        self.assertNotIn("--pyarrow_test", result.stdout)

    def test_specialized_test_modes_do_not_default_to_readiness_only(self):
        for mode in ("DELTA_LAKE_ONLY", "MULTITHREADED_SHUFFLE", "PYARROW_ONLY"):
            with self.subTest(mode=mode):
                result = self.run_script(mode=mode)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("PRODUCT_TEST", result.stdout)

    def test_prebuilt_default_and_split_modes(self):
        for mode in ("DEFAULT", "CI_PART1", "CI_PART2"):
            with self.subTest(mode=mode):
                result = self.run_script(mode=mode, jar_path="./prebuilt jars", upstream="0")
                self.assertEqual(result.returncode, 0, result.stderr)
                for args in ("-m delta_lake --delta_lake", "-k cache_test",
                             "-m shuffle_test", "-m pyarrow_test --pyarrow_test"):
                    self.assertEqual(args in result.stdout, mode != "CI_PART1")
                self.assertEqual("TEST_FILES json_test.py" in result.stdout, mode == "CI_PART2")

    def test_prebuilt_specialized_modes_launch_only_the_requested_suite(self):
        for mode, args in (("DELTA_LAKE_ONLY", "-m delta_lake --delta_lake"),
                           ("MULTITHREADED_SHUFFLE", "-m shuffle_test"),
                           ("PYARROW_ONLY", "-m pyarrow_test --pyarrow_test")):
            with self.subTest(mode=mode):
                result = self.run_script(mode=mode, jar_path="./prebuilt jars")
                self.assertEqual(result.returncode, 0, result.stderr)
                launches = [line for line in result.stdout.splitlines()
                            if line.startswith("PRODUCT_TEST ")]
                self.assertEqual(len(launches), 1, result.stdout)
                self.assertIn(args, launches[0])

    def test_prebuilt_delta_requires_a_passing_result_from_this_run(self):
        for report in ("", "<testcase name='test_delta_scan_read'><skipped/></testcase>"):
            with self.subTest(report=report):
                result = self.run_script(mode="DELTA_LAKE_ONLY", jar_path="./prebuilt jars",
                                         delta_report=report)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("No passing Delta Lake tests found", result.stderr)

    def test_prebuilt_delta_failure_preserves_pytest_exit_status(self):
        result = self.run_script(mode="DELTA_LAKE_ONLY", jar_path="./prebuilt jars", delta_exit=7)
        self.assertEqual(result.returncode, 7, result.stderr)

    def test_prebuilt_delta_retries_unsafe_report_directory_name(self):
        result = self.run_script(mode="DELTA_LAKE_ONLY", jar_path="./prebuilt jars",
                                 unsafe_delta_directory=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("run_dir-delta-abcdef", result.stdout)

    def test_invalid_mode_and_missing_prebuilt_path_fail(self):
        result = self.run_script(mode="UNSUPPORTED", jar_path="./prebuilt jars")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Unsupported TEST_MODE=UNSUPPORTED", result.stderr)
        self.assertNotIn("PRODUCT_TEST", result.stdout)

        result = self.run_script(extra_envs="LOCAL_JAR_PATH=missing")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Prebuilt jar directory does not exist: missing", result.stderr)
        self.assertNotIn("PRODUCT_TEST", result.stdout)


if __name__ == "__main__":
    unittest.main()

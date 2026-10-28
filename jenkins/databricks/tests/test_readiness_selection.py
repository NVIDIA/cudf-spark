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

"""Exercise shared CI readiness selection without installing or starting Spark."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class ReadinessSelectionTest(unittest.TestCase):
    def run_script(self, version="18.3.", mode="CI_PART1", hook=True,
                   override=None, extra_envs="", upstream="1", hook_exit=0):
        script = Path(__file__).resolve().parents[1] / "test.sh"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db_dir = root / "jenkins/databricks"
            db_dir.mkdir(parents=True)
            (db_dir / "setup.sh").write_text(":\n")
            (db_dir / "common_vars.sh").write_text(":\n")
            integration_dir = root / "integration_tests"
            integration_dir.mkdir()
            (integration_dir / "run_pyspark_from_build.sh").write_text(
                'echo "PRODUCT_TEST $*"\necho "TEST_FILES ${TESTS:-}"\n')
            startup = root / "startup.sh"
            startup.write_text(f'echo "READINESS $1"\nexit {hook_exit}\n')
            # Keep the actual control flow, with external setup/Spark calls stubbed.
            (root / "test.sh").write_text(script.read_text().replace(
                'SOURCE_PATH="/home/ubuntu/spark-rapids"', f'SOURCE_PATH="{root}"'))
            (root / "spark-3.3.0-bin-hadoop3").mkdir()
            env = {key: value for key, value in os.environ.items()
                   if key in {"PATH", "LANG"}}
            env.update(HOME=str(root), LOCAL_JAR_PATH=str(root),
                       BASE_SPARK_VERSION="3.3.0", TEST_MODE=mode,
                       EXTRA_ENVS=extra_envs, WITH_DEFAULT_UPSTREAM_SHIM=upstream,
                       PYSP_TEST_spark_databricks_clusterUsageTags_sparkVersion=version)
            if hook:
                env["SPARK_STARTUP_CHECK"] = str(startup)
            if override is not None:
                env["SPARK_READINESS_ONLY"] = override
            return subprocess.run(["bash", str(root / "test.sh")], env=env,
                                  text=True, capture_output=True, check=False)

    def test_ci_runs_non_delta_integration_tests_for_dbr_183(self):
        for mode in ("DEFAULT", "CI_PART1", "CI_PART2"):
            with self.subTest(mode=mode):
                result = self.run_script(mode=mode)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("READINESS databricks", result.stdout)
                self.assertEqual("READINESS upstream" in result.stdout, mode != "CI_PART2")
                self.assertIn("PRODUCT_TEST", result.stdout)
                self.assertNotIn("--delta_lake", result.stdout)

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

    def test_specialized_test_modes_do_not_default_to_readiness_only(self):
        for mode in ("DELTA_LAKE_ONLY", "MULTITHREADED_SHUFFLE", "PYARROW_ONLY"):
            with self.subTest(mode=mode):
                result = self.run_script(mode=mode)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("PRODUCT_TEST", result.stdout)


if __name__ == "__main__":
    unittest.main()

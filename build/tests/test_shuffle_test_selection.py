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

"""Exercise the real shuffle CI runner without starting Spark or accessing a GPU."""
import ast
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class ShuffleTestSelectionTests(unittest.TestCase):
    retry_test = "test_driverless_broadcast_recovers_after_probe_task_retry"

    def test_retry_filter_names_an_existing_integration_test(self):
        source = (Path(__file__).resolve().parents[2] /
                  "integration_tests/src/main/python/shuffle_broadcast_test.py")
        functions = {node.name for node in ast.parse(source.read_text()).body
                     if isinstance(node, ast.FunctionDef)}
        self.assertIn(self.retry_test, functions)

    def run_runner(self, mode, fail_main=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("nvidia-smi", "ucx_info"):
                stub = root / name
                stub.write_text("#!/bin/sh\nexit 0\n")
                stub.chmod(0o755)
            capture = root / "capture.py"
            capture.write_text("""#!/usr/bin/env python3
import json, os, sys
record = {'args': sys.argv[1:], 'failures': os.environ.get('SPARK_TASK_MAXFAILURES'),
          'spark_failures': os.environ.get('PYSP_TEST_spark_task_maxFailures')}
with open(os.environ['CAPTURE_PATH'], 'a') as stream:
    stream.write(json.dumps(record) + '\\n')
if os.environ.get('FAIL_MAIN') == '1' and any(a.startswith('not ') for a in sys.argv):
    sys.exit(7)
""")
            capture.chmod(0o755)
            output = root / "calls.jsonl"
            runner = Path(__file__).resolve().parents[2] / "jenkins/shuffle-common.sh"
            env = dict(os.environ, PATH=f"{root}:{os.environ['PATH']}",
                       SPARK_TASK_MAXFAILURES="1", PYSP_TEST_spark_task_maxFailures="1",
                       SPARK_MASTER="spark://test:7077", SPARK_VER="3.3.0",
                       CAPTURE_PATH=str(output), FAIL_MAIN="1" if fail_main else "0")
            result = subprocess.run(
                ["bash", "-c", 'source "$1"; invoke_shuffle_integration_test "$2" "$3" '
                 'premerge 3.3.0', "test", str(runner), mode, str(capture)],
                env=env, capture_output=True, text=True, timeout=20)
            calls = [json.loads(line) for line in output.read_text().splitlines()]
            return result, calls

    def test_ucx_keeps_fail_fast_tests_and_runs_retries_in_another_application(self):
        result, calls = self.run_runner("UCX")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], {
            "args": ["-m", "shuffle_test", "-k", "not " + self.retry_test],
            "failures": "1", "spark_failures": "1"})
        self.assertEqual(calls[1], {
            "args": ["-m", "shuffle_test", "-k", self.retry_test],
            "failures": "2", "spark_failures": "2"})

    def test_ordinary_failure_is_not_hidden_by_the_retry_application(self):
        result, calls = self.run_runner("UCX", fail_main=True)
        self.assertEqual(result.returncode, 7)
        self.assertEqual(len(calls), 1)

    def test_multithreaded_suite_remains_one_fail_fast_application(self):
        result, calls = self.run_runner("MULTITHREADED")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, [{"args": ["-m", "shuffle_test"],
                                  "failures": "1", "spark_failures": "1"}])


if __name__ == "__main__":
    unittest.main()

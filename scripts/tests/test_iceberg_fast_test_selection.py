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

import ast
import unittest
from pathlib import Path


ICEBERG_TEST_DIR = Path(__file__).parents[2] / \
    "integration_tests" / "src" / "main" / "python" / "iceberg"


def _is_skipif(call):
    return (isinstance(call.func, ast.Attribute) and call.func.attr == "skipif" and
            isinstance(call.func.value, ast.Attribute) and call.func.value.attr == "mark")


def _called_names(expression):
    return {
        node.func.id
        for node in ast.walk(expression)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


def _reason(call):
    reason = next((keyword.value for keyword in call.keywords if keyword.arg == "reason"), None)
    return reason.value if isinstance(reason, ast.Constant) else ""


class IcebergFastTestSelectionSuite(unittest.TestCase):
    def test_fast_mode_only_extends_runtime_reduction_skips(self):
        runtime_reduction_skips = 0
        catalog_specific_skips = 0
        for path in ICEBERG_TEST_DIR.glob("*_test.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for call in (node for node in ast.walk(tree)
                         if isinstance(node, ast.Call) and _is_skipif(node)):
                names = _called_names(call.args[0])
                if "is_iceberg_remote_catalog" not in names:
                    continue
                if "reduce test time" in _reason(call):
                    runtime_reduction_skips += 1
                    self.assertIn("is_iceberg_test_fast_run", names, path)
                else:
                    catalog_specific_skips += 1
                    self.assertNotIn("is_iceberg_test_fast_run", names, path)

        self.assertGreater(runtime_reduction_skips, 0)
        self.assertGreater(catalog_specific_skips, 0)


if __name__ == "__main__":
    unittest.main()

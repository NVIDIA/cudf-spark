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

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
IT_PYTHON_DIR = REPO_ROOT / "integration_tests" / "src" / "main" / "python"
sys.path.insert(0, str(IT_PYTHON_DIR))

# Pure unit tests isolated from PySpark, spark_session, and pytest
from spark_version import spark_version_tuple


class SparkVersionSuite(unittest.TestCase):
    def test_version_tuple_parsing(self):
        cases = [
            ("3.5.10", (3, 5, 10)),
            ("3.5.9", (3, 5, 9)),
            ("3.5.0", (3, 5, 0)),
            ("3.5.2", (3, 5, 2)),
            ("3.5.10-SNAPSHOT", (3, 5, 10)),
            ("3.5.2-amzn-0", (3, 5, 2)),
            ("3.5.10+nvidia", (3, 5, 10)),
            ("4.0.0-preview2", (4, 0, 0)),
            ("4.0.0.preview1", (4, 0, 0)),
            ("4.0.10", (4, 0, 10)),
            ("4.1.10", (4, 1, 10)),
            ("3.2.1.3.2.717", (3, 2, 1)),
            ("3.5", (3, 5, 0)),
            ("4", (4, 0, 0)),
            ((3, 5, 10), (3, 5, 10)),
        ]
        for ver_str, expected in cases:
            with self.subTest(version=ver_str):
                self.assertEqual(spark_version_tuple(ver_str), expected)

    def test_numeric_order_comparisons(self):
        # Issue #16678: string comparison "3.5.10" < "3.5.3" is True due to ASCII order.
        # Numeric tuple comparison must correctly order 3.5.10 after 3.5.3, 3.5.9, etc.
        self.assertGreater(spark_version_tuple("3.5.10"), spark_version_tuple("3.5.3"))
        self.assertGreater(spark_version_tuple("3.5.10"), spark_version_tuple("3.5.9"))
        self.assertGreaterEqual(spark_version_tuple("3.5.10"), spark_version_tuple("3.5.6"))
        self.assertLess(spark_version_tuple("3.5.10"), spark_version_tuple("3.6.0"))
        self.assertGreater(spark_version_tuple("4.0.10"), spark_version_tuple("4.0.4"))
        self.assertLess(spark_version_tuple("4.0.10"), spark_version_tuple("4.1.0"))

    def test_version_helpers_logic(self):
        # Test the version predicate logic against multi-digit and older releases
        def is_before_352(v):
            return spark_version_tuple(v) < (3, 5, 2)

        def is_before_353(v):
            return spark_version_tuple(v) < (3, 5, 3)

        def is_spark_353_or_later(v):
            return spark_version_tuple(v) >= (3, 5, 3)

        def is_spark_356_or_later(v):
            return spark_version_tuple(v) >= (3, 5, 6)

        def is_spark_350_or_351(v):
            return (3, 5, 0) <= spark_version_tuple(v) <= (3, 5, 1)

        def is_spark_35x(v):
            return (3, 5, 0) <= spark_version_tuple(v) < (3, 6, 0)

        def is_spark_40x(v):
            return (4, 0, 0) <= spark_version_tuple(v) < (4, 1, 0)

        def is_spark_41x(v):
            return (4, 1, 0) <= spark_version_tuple(v) < (4, 2, 0)

        # Spark 3.5.10 must sort as >= 3.5.3, >= 3.5.6, inside 3.5x, and not before 3.5.2 or 3.5.3
        self.assertFalse(is_before_352("3.5.10"))
        self.assertFalse(is_before_353("3.5.10"))
        self.assertTrue(is_spark_353_or_later("3.5.10"))
        self.assertTrue(is_spark_356_or_later("3.5.10"))
        self.assertFalse(is_spark_350_or_351("3.5.10"))
        self.assertTrue(is_spark_35x("3.5.10"))

        # Older releases
        self.assertTrue(is_before_352("3.5.1"))
        self.assertTrue(is_before_353("3.5.2"))
        self.assertFalse(is_spark_353_or_later("3.5.2"))
        self.assertTrue(is_spark_350_or_351("3.5.1"))

        # Spark 4.0.10 and 4.1.10
        self.assertTrue(is_spark_40x("4.0.10"))
        self.assertFalse(is_spark_41x("4.0.10"))
        self.assertTrue(is_spark_41x("4.1.10"))
        self.assertFalse(is_spark_40x("4.1.10"))


if __name__ == "__main__":
    unittest.main()

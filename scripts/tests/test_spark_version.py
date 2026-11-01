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

import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock

REPO_ROOT = Path(__file__).resolve().parents[2]
IT_PYTHON_DIR = REPO_ROOT / "integration_tests" / "src" / "main" / "python"
sys.path.insert(0, str(IT_PYTHON_DIR))

# Lightweight shims to allow importing spark_session helpers in pure unit test environments without Spark/JVM
if "pyspark" not in sys.modules:
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    sql_types = types.ModuleType("pyspark.sql.types")
    sql_types.TimestampType = MagicMock
    sql_types.DateType = MagicMock
    sql_types._acceptable_types = ()
    sql.DataFrame = MagicMock
    sql.Row = MagicMock
    sql.types = sql_types
    pyspark.sql = sql
    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = sql
    sys.modules["pyspark.sql.types"] = sql_types

    import spark_init_internal
    mock_spark = MagicMock()
    mock_spark.version = "3.5.10"
    mock_iter = MagicMock()
    mock_iter.hasNext.return_value = False
    mock_spark.conf._jconf.getAll.return_value.keys.return_value.iterator.return_value = mock_iter
    spark_init_internal._spark = mock_spark

import spark_session
from spark_session import (
    spark_version_tuple,
    is_before_spark_312,
    is_before_spark_320,
    is_before_spark_340,
    is_before_spark_350,
    is_before_spark_351,
    is_before_spark_352,
    is_before_spark_353,
    is_before_spark_400,
    is_spark_320_or_later,
    is_spark_330_or_later,
    is_spark_340_or_later,
    is_spark_350_or_later,
    is_spark_351_or_later,
    is_spark_353_or_later,
    is_spark_356_or_later,
    is_spark_350_or_351,
    is_spark_359,
    is_spark_35x,
    is_spark_40x,
    is_spark_41x,
    is_spark_400_or_later,
    is_spark_420_or_later,
    is_spark_500_or_later,
)


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

    def test_version_helpers_simulated_runtimes(self):
        matrix = [
            ("3.5.10", {
                "is_before_spark_352": False,
                "is_before_spark_353": False,
                "is_spark_353_or_later": True,
                "is_spark_356_or_later": True,
                "is_spark_350_or_351": False,
                "is_spark_359": False,
                "is_spark_35x": True,
                "is_before_spark_400": True,
                "is_spark_400_or_later": False,
            }),
            ("3.5.2", {
                "is_before_spark_352": False,
                "is_before_spark_353": True,
                "is_spark_353_or_later": False,
                "is_spark_356_or_later": False,
                "is_spark_350_or_351": False,
                "is_spark_359": False,
                "is_spark_35x": True,
                "is_before_spark_400": True,
                "is_spark_400_or_later": False,
            }),
            ("3.5.1", {
                "is_before_spark_352": True,
                "is_before_spark_353": True,
                "is_spark_353_or_later": False,
                "is_spark_356_or_later": False,
                "is_spark_350_or_351": True,
                "is_spark_359": False,
                "is_spark_35x": True,
                "is_before_spark_400": True,
                "is_spark_400_or_later": False,
            }),
            ("4.0.10", {
                "is_before_spark_400": False,
                "is_spark_400_or_later": True,
                "is_spark_40x": True,
                "is_spark_41x": False,
                "is_spark_35x": False,
            }),
            ("4.1.10", {
                "is_spark_40x": False,
                "is_spark_41x": True,
                "is_spark_420_or_later": False,
            }),
        ]
        orig_spark_version = spark_session.spark_version
        try:
            for ver_str, expected_helpers in matrix:
                spark_session.spark_version = lambda v=ver_str: v
                for helper_name, expected_val in expected_helpers.items():
                    with self.subTest(version=ver_str, helper=helper_name):
                        helper_fn = getattr(spark_session, helper_name)
                        self.assertEqual(
                            helper_fn(),
                            expected_val,
                            f"Helper {helper_name}() failed for version {ver_str}",
                        )
        finally:
            spark_session.spark_version = orig_spark_version


if __name__ == "__main__":
    unittest.main()

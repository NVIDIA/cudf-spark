# Copyright (c) 2025, NVIDIA CORPORATION.
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

import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from src.main.python.marks import ignore_order

_presplit_conf = {'spark.rapids.sql.test.overrides.splitUntilSize': 10}

@ignore_order(local=True)
def test_project_create_array():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, int_gen, string_gen).selectExpr(
            "array(a, 1)", "array(b, 's1')"),
        _presplit_conf)

@ignore_order(local=True)
def test_project_create_struct():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, int_gen, string_gen).selectExpr(
            "struct(a, b, 1, 's1')"),
        _presplit_conf)

@ignore_order(local=True)
def test_project_create_map():
    nonull_string_gen = StringGen(nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, int_gen, nonull_string_gen).selectExpr(
            "map(b, a, 's1', 1)"),
        _presplit_conf)



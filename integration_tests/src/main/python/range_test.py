# Copyright (c) 2020, NVIDIA CORPORATION.
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
from data_gen import LONG_MAX, LONG_MIN
from pyspark.sql.types import *
import pyspark.sql.functions as f

def test_simple_range():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.range(100))

def test_start_end_range():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.range(-100, 100))

def test_step_range():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.range(-100, 100, 7))

def test_neg_step_range():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.range(100, -100, -7))

def test_partitioned_range():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.range(1000, numPartitions=2))

def test_large_corner_range():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.range(LONG_MAX - 100, LONG_MAX, step=3))

def test_small_corner_range():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.range(LONG_MIN + 100, LONG_MIN, step=-3))

def test_wrong_step_corner_range():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.range(100, -100, 7))



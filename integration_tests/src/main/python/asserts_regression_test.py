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

import pytest

from asserts import assert_equal
from spark_session import with_cpu_session, with_gpu_session


def test_assert_equal_row_count_match():
    cpu_count = with_cpu_session(lambda spark: spark.range(12).count())
    gpu_count = with_gpu_session(lambda spark: spark.range(12).count())

    assert_equal(cpu_count, gpu_count)


def test_assert_equal_row_count_mismatch_raises_assertion_error(capsys):
    cpu_count = with_cpu_session(lambda spark: spark.range(2).count())
    gpu_count = with_gpu_session(lambda spark: spark.range(1).count())

    with pytest.raises(AssertionError) as exc_info:
        assert_equal(cpu_count, gpu_count)

    assert "int values are different" in str(exc_info.value)
    captured = capsys.readouterr()
    assert "--- CPU OUTPUT" in captured.out
    assert "+++ GPU OUTPUT" in captured.out
    diff_lines = captured.out.splitlines()
    assert "-2" in diff_lines
    assert "+1" in diff_lines

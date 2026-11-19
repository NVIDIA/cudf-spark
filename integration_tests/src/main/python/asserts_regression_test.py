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

from asserts import assert_equal, collect_plan_nodes
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


@pytest.mark.parametrize('deduplicate', [False, True])
def test_collect_plan_nodes_handles_adaptive_stages_and_reuse(deduplicate):
    # The fixture deliberately keeps wrappers opaque to ordinary children traversal.
    # Removing any wrapper handler must lose the producer and fail the assertion.
    from types import SimpleNamespace

    class Nodes:
        def __init__(self, values):
            self.values = iter(values)
            self.pending = None

        def hasNext(self):
            if self.pending is None:
                self.pending = next(self.values, None)
            return self.pending is not None

        def next(self):
            result = self.pending
            self.pending = None
            return result

    class Node:
        def __init__(self, name, identity, children=(), wrapped=None):
            self.name, self.identity, self.nested, self.wrapped = name, identity, children, wrapped

        def getClass(self):
            return SimpleNamespace(getSimpleName=lambda: self.name, getName=lambda: self.name)

        def id(self):
            return self.identity

        def children(self):
            return SimpleNamespace(iterator=lambda: Nodes(self.nested))

        def executedPlan(self):
            return self.wrapped

        def plan(self):
            return self.wrapped

        def child(self):
            return self.wrapped

    producer = Node('GpuShuffleExchangeExec', 1)
    reused = Node('ReusedExchangeExec', 2, wrapped=producer)
    stage = Node('BroadcastQueryStageExec', 3, wrapped=reused)
    root = Node('AdaptiveSparkPlanExec', 4,
                wrapped=Node('UnionExec', 5, children=[stage, stage]))
    assert collect_plan_nodes(root) == [root]
    result = collect_plan_nodes(root, include_wrapped=True, deduplicate=deduplicate)
    assert result.count(producer) == (1 if deduplicate else 2)
    assert result.count(reused) == (1 if deduplicate else 2)
    assert result.count(stage) == (1 if deduplicate else 2)

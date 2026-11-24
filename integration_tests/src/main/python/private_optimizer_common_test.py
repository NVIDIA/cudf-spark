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

"""Unit tests for the plan-comparison helpers in private_optimizer_common.

These stub out the Spark sessions so they verify the helpers themselves: that a
no-op rule passes and that a rule which changes the plan, or a missing required
marker, fails instead of passing silently.
"""

import pytest
from pyspark.sql import Row

import private_optimizer_common as common

ON_CONF = {"rule": "on"}
OFF_CONF = {"rule": "off"}
ROWS = [Row(cnt=1), Row(cnt=2)]
SKEW_READER = "GpuCustomShuffleReader coalesced and skewed"
JOIN_MARKER = "GpuBuildRight, false, true"

PLAN_OFF = """GpuHashAggregate (keys=[], functions=[gpucount(1, true), gpubasicsum(value1#5L)]) [loreId=47]
+- GpuBroadcastHashJoin [key1#4L], [key2#1L], Inner, GpuBuildRight, false, true, [loreId=40]
   :- GpuCustomShuffleReader coalesced and skewed
   :  +- ShuffleQueryStage 0, Statistics(sizeInBytes=26.3 KiB, rowCount=1.00E+3, ColumnStat: N/A, isRuntime=true)
   :     +- GpuColumnarExchange gpuhashpartitioning(key1#4L, 100), REPARTITION_BY_NUM, [plan_id=120], [loreId=16]
   +- ShuffleQueryStage 1, Statistics(sizeInBytes=31.6 KiB, rowCount=2.00E+3, ColumnStat: N/A, isRuntime=true)
"""
# Same structure as PLAN_OFF with different expression ids, plan ids, lore ids and statistics.
PLAN_ON_SAME = """GpuHashAggregate (keys=[], functions=[gpucount(1, true), gpubasicsum(value1#268L)]) [loreId=99]
+- GpuBroadcastHashJoin [key1#267L], [key2#264L], Inner, GpuBuildRight, false, true, [loreId=88]
   :- GpuCustomShuffleReader coalesced and skewed
   :  +- ShuffleQueryStage 0, Statistics(sizeInBytes=26.4 KiB, rowCount=1.00E+3, ColumnStat: N/A, isRuntime=true)
   :     +- GpuColumnarExchange gpuhashpartitioning(key1#267L, 100), REPARTITION_BY_NUM, [plan_id=476], [loreId=77]
   +- ShuffleQueryStage 1, Statistics(sizeInBytes=31.7 KiB, rowCount=2.00E+3, ColumnStat: N/A, isRuntime=true)
"""


def _stub_sessions(monkeypatch, gpu_off_plan, gpu_on_plan, gpu_rows=ROWS):
    """Replace the session helpers so each conf yields a canned (rows, plan)."""
    results = {
        ("cpu", "off"): (ROWS, "cpu-off-plan"),
        ("gpu", "off"): (ROWS, gpu_off_plan),
        ("gpu", "on"): (gpu_rows, gpu_on_plan),
    }

    def session(kind):
        def run(fn, conf=None):
            return results[(kind, conf["rule"])]
        return run

    monkeypatch.setattr(common, "with_cpu_session", session("cpu"))
    monkeypatch.setattr(common, "with_gpu_session", session("gpu"))
    monkeypatch.setattr(common, "assert_equal_with_local_sort",
                        lambda cpu, gpu: _assert_rows_equal(cpu, gpu))


def _assert_rows_equal(cpu, gpu):
    assert sorted(map(tuple, cpu)) == sorted(map(tuple, gpu)), "rows differ"


def _noop(monkeypatch, on_plan, off_plan=PLAN_OFF, required=(JOIN_MARKER,), gpu_rows=ROWS):
    _stub_sessions(monkeypatch, off_plan, on_plan, gpu_rows)
    common.assert_rule_noop_on_gpu(lambda s: None, ON_CONF, OFF_CONF,
                                   required_on_markers=required, physical=True)


@pytest.mark.private_optimizer
def test_noop_passes_when_plans_differ_only_in_volatile_details(monkeypatch):
    _noop(monkeypatch, PLAN_ON_SAME)


@pytest.mark.private_optimizer
def test_noop_passes_when_both_plans_contain_native_marker(monkeypatch):
    # The runtime produces the rule's marker itself, so its presence alone must
    # not fail the check as long as ON and OFF agree.
    assert SKEW_READER in PLAN_OFF and SKEW_READER in PLAN_ON_SAME
    _noop(monkeypatch, PLAN_ON_SAME)


@pytest.mark.private_optimizer
def test_noop_fails_when_rule_adds_a_plan_node(monkeypatch):
    off_without_skew = PLAN_OFF.replace(
        "   :- GpuCustomShuffleReader coalesced and skewed\n", "")
    with pytest.raises(AssertionError, match="expected a no-op"):
        _noop(monkeypatch, PLAN_ON_SAME, off_plan=off_without_skew)


@pytest.mark.private_optimizer
def test_noop_fails_when_rule_changes_plan_structure(monkeypatch):
    changed = PLAN_ON_SAME.replace("Inner, GpuBuildRight", "Inner, GpuBuildLeft")
    with pytest.raises(AssertionError, match="expected a no-op"):
        _noop(monkeypatch, changed, required=())


@pytest.mark.private_optimizer
def test_noop_fails_when_required_marker_missing(monkeypatch):
    with pytest.raises(AssertionError, match="required marker"):
        _noop(monkeypatch, PLAN_ON_SAME, required=("GpuNoSuchMarker",))


@pytest.mark.private_optimizer
def test_noop_fails_when_results_differ(monkeypatch):
    with pytest.raises(AssertionError, match="rows differ"):
        _noop(monkeypatch, PLAN_ON_SAME, gpu_rows=[Row(cnt=1), Row(cnt=3)])


@pytest.mark.private_optimizer
def test_normalize_plan_strips_only_volatile_details():
    normalized = common._normalize_plan(PLAN_OFF)
    assert "#4L" not in normalized and "plan_id=120" not in normalized
    assert "loreId=47" not in normalized and "26.3 KiB" not in normalized
    # structure and names survive
    assert "key1#" in normalized and "GpuBuildRight, false, true" in normalized
    assert common._normalize_plan(PLAN_OFF) == common._normalize_plan(PLAN_ON_SAME)

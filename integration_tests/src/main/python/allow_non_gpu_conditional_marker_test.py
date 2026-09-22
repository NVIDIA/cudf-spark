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

# Contract and hardening tests for the allow_non_gpu_conditional pytest marker.
#
# Every test asserts the marker's documented contract:
#   @allow_non_gpu_conditional(condition, *ops, any=False)
#   - condition must be a literal bool; anything else (including a missing
#     condition) raises TypeError on BOTH branches
#   - a false condition contributes nothing (state-equivalent to no marker)
#   - ops are one or more positional strings, each optionally comma-separated;
#     entries are trimmed, empties dropped, duplicates removed
#   - any=True (condition true) allows anything and takes precedence over ops;
#     'any' must be a real bool and is the only accepted keyword
#
# The tests are Spark-job-free: marker processing happens in the real
# conftest.pytest_runtest_setup (driven either by real pytest markers or by a
# direct call with a minimal fake item), and no test body issues Spark work.
# Module-scope marker coverage lives in allow_non_gpu_conditional_scope_test.py
# so the pytestmark there cannot taint the unmarked baselines here.

import warnings

import pytest

import conftest
from conftest import get_non_gpu_allowed, is_allowing_any_non_gpu
from marks import allow_non_gpu, allow_non_gpu_conditional


def allowed():
    return list(get_non_gpu_allowed())


# ---------------------------------------------------------------------------
# Baseline behavior
# ---------------------------------------------------------------------------

def test_baseline_no_marker():
    assert allowed() == [], f"no marker must allow nothing, got {allowed()}"
    assert not is_allowing_any_non_gpu()


@allow_non_gpu_conditional(True, "CondTrueOp")
def test_true_condition_applies_ops():
    assert "CondTrueOp" in allowed(), \
        f"true condition must apply its ops, got {allowed()}"
    assert not is_allowing_any_non_gpu()


@allow_non_gpu("BaseOp")
@allow_non_gpu_conditional(True, "CondOp")
def test_true_condition_merges_with_base():
    assert "BaseOp" in allowed() and "CondOp" in allowed(), \
        f"true condition must merge with the base allow list, got {allowed()}"


@allow_non_gpu_conditional(True, "AnyWithOps", any=True)
def test_any_true_with_ops_precedence():
    # any=True takes precedence over an ops payload (mirrors allow_non_gpu).
    assert is_allowing_any_non_gpu(), "any=True with a true condition must allow anything"
    assert "AnyWithOps" not in allowed(), \
        f"ops payload must be superseded by any=True, got {allowed()}"


# ---------------------------------------------------------------------------
# False conditions must contribute no allowances
# ---------------------------------------------------------------------------

@allow_non_gpu_conditional(False, "LeakedOp")
def test_false_condition_adds_nothing():
    assert "LeakedOp" not in allowed(), \
        f"LEAK: ops of a false-condition marker are applied, got {allowed()}"
    assert not is_allowing_any_non_gpu()


@allow_non_gpu("KeptOp")
@allow_non_gpu_conditional(False, "LeakA,LeakB")
def test_false_condition_keeps_base_allowances_intact():
    assert allowed() == ["KeptOp"], \
        f"false condition must leave exactly the base allowances, got {allowed()}"


@allow_non_gpu_conditional(False, "LeakedAnyOp", any=True)
def test_false_condition_with_any_true():
    assert not is_allowing_any_non_gpu(), "any=True must be gated by the condition"
    assert "LeakedAnyOp" not in allowed(), \
        f"LEAK: ops of a false-condition any=True marker applied, got {allowed()}"


# ---------------------------------------------------------------------------
# Condition-only and any-only invocation forms must set up cleanly
# ---------------------------------------------------------------------------

@allow_non_gpu_conditional(True)
def test_condition_only_true_warns_not_errors():
    # A condition-only marker allows nothing (plus a warning), like a bare
    # @allow_non_gpu. Pre-fix, setup died with IndexError on args[1].
    assert allowed() == [], f"condition-only marker must allow nothing, got {allowed()}"
    assert not is_allowing_any_non_gpu()


@allow_non_gpu_conditional(True, any=True)
def test_any_true_without_ops():
    # The documented (condition, any=True) form. Pre-fix, setup died with
    # IndexError on args[1] before kwargs were even inspected.
    assert is_allowing_any_non_gpu(), "(True, any=True) must allow anything on the CPU"
    assert allowed() == []


# ---------------------------------------------------------------------------
# Varargs, stacked markers, and payload trimming
# ---------------------------------------------------------------------------

@allow_non_gpu_conditional(True, "MultiA", "MultiB")
def test_multiple_ops_varargs():
    # Varargs ops are accepted like allow_non_gpu. Pre-fix, args[2:] were
    # silently ignored.
    assert "MultiA" in allowed() and "MultiB" in allowed(), \
        f"all ops args must be honored, got {allowed()}"


@allow_non_gpu_conditional(True, "StackTop")
@allow_non_gpu_conditional(True, "StackBottom")
def test_stacked_markers_union():
    # Every stacked marker contributes (each gated by its own condition).
    # Pre-fix, get_closest_marker silently dropped all but one.
    assert "StackTop" in allowed() and "StackBottom" in allowed(), \
        f"stacked markers must union their allowances, got {allowed()}"


@allow_non_gpu_conditional(True, "SpaceA, SpaceB")
def test_embedded_space_trimmed():
    # Entries are trimmed on split so Python-side consumers of
    # get_non_gpu_allowed() never see ' SpaceB'. (The Scala side already
    # trims via ConfHelper.stringToSeq.)
    assert "SpaceB" in allowed() and " SpaceB" not in allowed(), \
        f"payload entries must be trimmed, got {allowed()}"


# ---------------------------------------------------------------------------
# Raising/warning forms, exercised by direct call with a minimal fake item
# ---------------------------------------------------------------------------

class _FakeMark:
    def __init__(self, args, kwargs=None):
        self.args = args
        self.kwargs = kwargs or {}


class _FakeItem:
    """Minimal stand-in carrying only allow_non_gpu_conditional marks."""

    def __init__(self, *marks):
        self._marks = list(marks)

    def get_closest_marker(self, name):
        if name == "allow_non_gpu_conditional" and self._marks:
            return self._marks[0]
        return None

    def iter_markers(self, name):
        if name == "allow_non_gpu_conditional":
            return list(self._marks)
        return []


def _setup(*marks):
    conftest.pytest_runtest_setup(_FakeItem(*marks))


def test_non_bool_condition_with_ops_raises_typeerror():
    with pytest.raises(TypeError, match="must be a Boolean"):
        _setup(_FakeMark(("premerge", "OpY")))


def test_non_bool_condition_alone_raises_typeerror_not_indexerror():
    # The boolean type check fires for every malformed invocation. Pre-fix, a
    # condition-only non-bool marker died with IndexError on args[1] before
    # the TypeError check was reached.
    with pytest.raises(TypeError, match="must be a Boolean"):
        _setup(_FakeMark(("premerge",)))


# ---------------------------------------------------------------------------
# any= must be a real bool: a truthy non-bool must never disable test mode
# ---------------------------------------------------------------------------

def test_nonbool_any_string_rejected():
    with pytest.raises(TypeError, match="'any' parameter"):
        _setup(_FakeMark((True, "OpA"), {"any": "false"}))
    assert not is_allowing_any_non_gpu(), "truthy string any= must not disable test mode"


def test_nonbool_any_int_rejected():
    with pytest.raises(TypeError, match="'any' parameter"):
        _setup(_FakeMark((True, "OpA"), {"any": 1}))
    assert not is_allowing_any_non_gpu()


# ---------------------------------------------------------------------------
# Unknown / typo kwargs must be rejected, not silently ignored
# ---------------------------------------------------------------------------

def test_unknown_kwarg_rejected():
    with pytest.raises(TypeError, match="unexpected keyword"):
        _setup(_FakeMark((True, "OpA"), {"reason": "some reason"}))


def test_typo_kwarg_rejected():
    with pytest.raises(TypeError, match="unexpected keyword"):
        _setup(_FakeMark((True,), {"anny": True}))


# ---------------------------------------------------------------------------
# Argument-shape regressions
# ---------------------------------------------------------------------------

def test_zero_argument_marker_rejected():
    # Bare @allow_non_gpu_conditional(): no condition at all.
    with pytest.raises(TypeError, match="requires a Boolean condition"):
        _setup(_FakeMark(()))


@allow_non_gpu_conditional(True, "ExplicitAnyFalseOp", any=False)
def test_explicit_any_false_with_ops():
    # any=False is the documented default spelled out: ops apply normally.
    assert "ExplicitAnyFalseOp" in allowed(), f"got {allowed()}"
    assert not is_allowing_any_non_gpu()


# ---------------------------------------------------------------------------
# Ops must be strings; validation fires on both condition branches
# ---------------------------------------------------------------------------

def test_nonstring_op_rejected():
    with pytest.raises(TypeError, match="must be strings"):
        _setup(_FakeMark((True, 123)))


def test_malformed_rejected_even_when_condition_false():
    with pytest.raises(TypeError, match="must be strings"):
        _setup(_FakeMark((False, 123)))
    assert allowed() == [], "false-branch malformed marker must not mutate state"


# ---------------------------------------------------------------------------
# Empty / whitespace payloads warn instead of silently allowing nothing
# ---------------------------------------------------------------------------

def test_active_empty_payload_warns():
    with pytest.warns(UserWarning, match="empty ops payload"):
        _setup(_FakeMark((True, "")))
    assert allowed() == []
    assert not is_allowing_any_non_gpu()


def test_whitespace_payload_warns():
    with pytest.warns(UserWarning, match="empty ops payload"):
        _setup(_FakeMark((True, "  ,  ")))
    assert allowed() == []


# ---------------------------------------------------------------------------
# Condition-only forms: true warns, false is silent (exactly like no marker)
# ---------------------------------------------------------------------------

def test_true_condition_only_warns():
    with pytest.warns(UserWarning, match="without anything allowed"):
        _setup(_FakeMark((True,)))
    assert allowed() == []


def test_false_condition_only_is_silent():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _setup(_FakeMark((False,)))
    ours = [w for w in caught if "allow_non_gpu_conditional" in str(w.message)]
    assert not ours, f"false condition-only must be silent, got {ours}"
    assert allowed() == []


# ---------------------------------------------------------------------------
# Duplicate ops across stacked markers deduplicate ("union" semantics)
# ---------------------------------------------------------------------------

def test_duplicate_ops_deduplicated():
    _setup(_FakeMark((True, "DupOp,DupOp")), _FakeMark((True, "DupOp")))
    assert allowed() == ["DupOp"], \
        f"union semantics require dedup, got {allowed()}"

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

"""Pin the reduced IT (each-choice) selection in conftest against pytest changes.

conftest._reduced_it_required_items relies on pytest emitting stacked
``parametrize`` cases in Cartesian-product order (higher iter_markers position
varies fastest). If a future pytest changes that order, the each-choice
coverage guarantee would break silently -- tests would still pass while some
parameter values stop running in pre-commit. This test forces a real pytest
collection and asserts, for every synthetic layout, that:

  * the decoded per-decorator indices match each item's real callspec values
    (this is the ordering assumption), and
  * every value of every decorator appears in at least one kept item, using the
    minimum number of cases (max factor size), and
  * tests with fewer than two parametrize decorators, or non-Cartesian
    (iterator) parametrization, are kept in full.

The real collection runs in a subprocess so it never nests an in-process pytest
run inside the integration-test session that collects this file.
"""

import os
import subprocess
import sys

import pytest

# Synthetic layouts. Each source stacks parametrize decorators with distinct
# value prefixes so a case id like ``[a1-b0-c2]`` reveals every real value.
_SYNTHETIC_SOURCES = {
    # (reduced?, expected_kept) keyed by test-function name is asserted below.
    "test_three_distinct": '''
import pytest
@pytest.mark.parametrize("c", ["c0", "c1", "c2"])
@pytest.mark.parametrize("b", ["b0", "b1"])
@pytest.mark.parametrize("a", ["a0", "a1", "a2", "a3"])
def test_three_distinct(a, b, c):
    pass
''',
    "test_tie": '''
import pytest
@pytest.mark.parametrize("y", ["y0", "y1", "y2"])
@pytest.mark.parametrize("x", ["x0", "x1", "x2"])
def test_tie(x, y):
    pass
''',
    "test_largest_middle": '''
import pytest
@pytest.mark.parametrize("r", ["r0", "r1", "r2"])
@pytest.mark.parametrize("q", ["q0", "q1", "q2", "q3", "q4"])
@pytest.mark.parametrize("p", ["p0", "p1"])
def test_largest_middle(p, q, r):
    pass
''',
    "test_module_level": '''
import pytest
pytestmark = pytest.mark.parametrize("m", ["m0", "m1", "m2"])
@pytest.mark.parametrize("f", ["f0", "f1"])
def test_module_level(f, m):
    pass
''',
    "test_multi_arg": '''
import pytest
@pytest.mark.parametrize("d", ["d0", "d1"])
@pytest.mark.parametrize("a,b", [("a0", "b0"), ("a1", "b1"), ("a2", "b2")])
def test_multi_arg(a, b, d):
    pass
''',
    "test_keyword_args": '''
import pytest
@pytest.mark.parametrize("y", ["y0", "y1"])
@pytest.mark.parametrize(argnames="x", argvalues=["x0", "x1", "x2"])
def test_keyword_args(x, y):
    pass
''',
    "test_single": '''
import pytest
@pytest.mark.parametrize("s", ["s0", "s1", "s2"])
def test_single(s):
    pass
''',
    "test_none": '''
import pytest
def test_none():
    pass
''',
    "test_iterator": '''
import pytest
@pytest.mark.parametrize("k", ["k0", "k1"])
@pytest.mark.parametrize("g", (v for v in ["g0", "g1", "g2"]))
def test_iterator(g, k):
    pass
''',
}

# function name -> (expected each-choice reduction happened?, expected kept count)
_EXPECTATIONS = {
    "test_three_distinct": (True, 4),
    "test_tie": (True, 3),
    "test_largest_middle": (True, 5),
    "test_module_level": (True, 3),
    "test_multi_arg": (True, 3),
    "test_keyword_args": (True, 3),
    "test_single": (False, 3),
    "test_none": (False, 1),
    # Kept in full because a generator argvalues has no recoverable len(), so
    # _precommit_parametrize_factors bails out. If a future pytest materializes
    # iterator argvalues into a sized sequence, this case would start reducing
    # and turn red -- that is an intended signal to re-review the fallback, not
    # a flaky failure.
    "test_iterator": (False, 6),
}


def _run_selftest():
    """Collect the synthetic suite for real and validate the selection.

    Returns a list of human-readable failure strings (empty means success).
    Only invoked in the subprocess (``__main__``), never during collection.
    """
    import tempfile

    conftest_dir = os.path.dirname(os.path.abspath(__file__))
    if conftest_dir not in sys.path:
        sys.path.insert(0, conftest_dir)
    import conftest  # the module under test

    failures = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for name, source in _SYNTHETIC_SOURCES.items():
            with open(os.path.join(tmpdir, name + ".py"), "w") as handle:
                handle.write(source)

        collected = []
        selected = []
        deselected = []

        class _Capture:
            @pytest.hookimpl(hookwrapper=True, tryfirst=True)
            def pytest_collection_modifyitems(self, items):
                collected.extend(items)
                yield
                selected.extend(items)

            def pytest_deselected(self, items):
                deselected.extend(items)

        class _ReducedIT:
            def pytest_addoption(self, parser):
                parser.addoption("--test_oom_injection_mode", default="never")

            @pytest.hookimpl(trylast=True)
            def pytest_collection_modifyitems(self, config, items):
                conftest._is_precommit_run = True
                conftest.pytest_collection_modifyitems(config, items)

        # Isolated collection: tmpdir has no conftest, so the integration-test
        # conftest and the spark plugin are not loaded here. Invoke the real
        # collection hook through a minimal plugin to exercise reduced-IT
        # activation, item mutation, and deselection notification.
        os.environ["REDUCED_IT"] = "true"
        rc = pytest.main(
            [tmpdir, "--collect-only", "-q",
             "--rootdir", tmpdir, "-p", "no:cacheprovider", "-o", "addopts="],
            plugins=[_ReducedIT(), _Capture()])
        if rc != 0:
            return [f"synthetic collection failed with pytest rc={rc}"]

        # Group in collection order, exactly like _reduced_it_required_items.
        groups = {}
        for item in collected:
            groups.setdefault(item.nodeid.split("[", 1)[0], []).append(item)

        required = set(selected)
        helper_required, each_choice_test_count = conftest._reduced_it_required_items(collected)
        if helper_required != required:
            failures.append("collection hook selection differs from the pure reducer")
        if set(deselected) != set(collected) - required:
            failures.append("pytest_deselected items differ from the removed collection items")
        reduced_names = set()

        for prefix, group_items in groups.items():
            func_name = prefix.rsplit("::", 1)[-1]
            expect_reduced, expect_kept = _EXPECTATIONS[func_name]
            first = group_items[0]
            factors = conftest._precommit_parametrize_factors(first)
            marks = [
                (mark.args[0] if mark.args else mark.kwargs["argnames"],
                 list(mark.args[1] if len(mark.args) > 1 else mark.kwargs["argvalues"]))
                for mark in first.iter_markers(name="parametrize")
            ]
            kept = [item for item in group_items if item in required]

            if not expect_reduced:
                if set(group_items) - required:
                    failures.append(f"{func_name}: expected all {len(group_items)} "
                                    f"cases kept, kept {len(kept)}")
                continue

            reduced_names.add(func_name)

            # 1) Ordering pin: decoded indices must equal the real callspec values.
            for position, item in enumerate(group_items):
                decoded = conftest._combination_for_position(position, factors)
                real = tuple(marks[factor[1]][1].index(_value_id(item, marks[factor[1]][0]))
                             for factor in factors)
                if decoded != real:
                    failures.append(f"{func_name}: position {position} decoded {decoded} "
                                    f"!= real {real} (pytest ordering changed?)")

            # 2) Minimality: kept count equals the largest factor's size.
            if len(kept) != expect_kept:
                failures.append(f"{func_name}: kept {len(kept)} cases, expected {expect_kept}")

            # 3) Coverage: every value of every decorator appears among kept cases.
            for names, values in marks:
                for value in values:
                    covered = any(_value_id(item, names) == value for item in kept)
                    if not covered:
                        failures.append(f"{func_name}: value {names}={value!r} never kept")

        if each_choice_test_count != len(reduced_names):
            failures.append(f"each_choice_test_count={each_choice_test_count} "
                            f"but reduced {len(reduced_names)} tests")

        # 4) Determinism: a second call keeps exactly the same node ids.
        required_again, _ = conftest._reduced_it_required_items(collected)
        if {item.nodeid for item in required} != {item.nodeid for item in required_again}:
            failures.append("selection is not deterministic across calls")

    return failures


def _value_id(item, argname):
    """Return the value the item bound to ``argname`` (or the ``a,b`` tuple key)."""
    params = item.callspec.params
    names = argname.split(",")
    if len(names) == 1:
        return params[names[0]]
    return tuple(params[name] for name in names)


@pytest.mark.parametrize(
    "precommit,reduced_it,expected_path",
    [
        (False, "true", "random"),
        (True, None, "random"),
        (True, "false", "random"),
        (True, "true", "reduced"),
    ])
def test_reduced_it_activation(monkeypatch, precommit, reduced_it, expected_path):
    """Require both precommit and REDUCED_IT to activate reduced selection."""
    import conftest

    monkeypatch.setattr(conftest, "is_precommit_run", lambda: precommit)
    if reduced_it is None:
        monkeypatch.delenv("REDUCED_IT", raising=False)
    else:
        monkeypatch.setenv("REDUCED_IT", reduced_it)

    selected_paths = []
    monkeypatch.setattr(
        conftest, "_select_precommit_cases",
        lambda config, items: selected_paths.append("reduced"))
    monkeypatch.setattr(
        conftest, "_maybe_apply_random_select",
        lambda config, items: selected_paths.append("random"))

    conftest.pytest_collection_modifyitems(None, [])

    assert selected_paths == [expected_path]


def test_reduced_it_each_choice_selection():
    """Run the isolated collection self-test in a subprocess and require success."""
    # Clear PYTEST_ADDOPTS so a caller's global pytest options (extra plugins,
    # xdist, etc.) cannot leak into the isolated synthetic collection below.
    env = {**os.environ, "PYTEST_ADDOPTS": ""}
    result = subprocess.run(
        [sys.executable, os.path.abspath(__file__)],
        capture_output=True, text=True, env=env)
    assert result.returncode == 0, (
        "reduced IT selection self-test failed:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}")


if __name__ == "__main__":
    _failures = _run_selftest()
    for _failure in _failures:
        print("FAIL:", _failure)
    sys.exit(1 if _failures else 0)

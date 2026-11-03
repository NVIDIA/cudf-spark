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

# Scope tests for the allow_non_gpu_conditional pytest marker:
# iter_markers accumulates function, parameter, class, and module marks; every
# effective marker in scope contributes (each gated by its own condition), with
# entries deduplicated. Pre-fix, get_closest_marker kept only the closest mark
# and outer scopes were silently dropped.
#
# This module is separate from allow_non_gpu_conditional_marker_test.py because
# the module-level pytestmark below applies to every test in this file and must
# not taint the unmarked baseline tests there. The tests are Spark-job-free.

import pytest

from conftest import get_non_gpu_allowed
from marks import allow_non_gpu_conditional

# Module-scope marker: applies to every test in this file.
pytestmark = allow_non_gpu_conditional(True, "ModuleScopeOp")


def allowed():
    return list(get_non_gpu_allowed())


def test_module_scope_applies():
    assert "ModuleScopeOp" in allowed(), f"got {allowed()}"


@allow_non_gpu_conditional(True, "FuncScopeOp")
def test_module_plus_function_scope():
    a = allowed()
    assert "FuncScopeOp" in a and "ModuleScopeOp" in a, \
        f"module + function markers must both contribute, got {a}"


class TestClassScope:
    pytestmark = allow_non_gpu_conditional(True, "ClassScopeOp")

    def test_module_plus_class_scope(self):
        a = allowed()
        assert "ClassScopeOp" in a and "ModuleScopeOp" in a, \
            f"module + class markers must both contribute, got {a}"

    @allow_non_gpu_conditional(True, "FuncScopeOp2")
    def test_module_class_function_scope(self):
        a = allowed()
        assert {"ModuleScopeOp", "ClassScopeOp", "FuncScopeOp2"} <= set(a), \
            f"all scopes must contribute, got {a}"


@pytest.mark.parametrize("x", [
    pytest.param(1, marks=allow_non_gpu_conditional(True, "ParamScopeOp")),
    pytest.param(2),
])
def test_param_scope(x):
    a = allowed()
    if x == 1:
        assert "ParamScopeOp" in a and "ModuleScopeOp" in a, f"got {a}"
    else:
        assert "ParamScopeOp" not in a and "ModuleScopeOp" in a, f"got {a}"


@allow_non_gpu_conditional(True, "ModuleScopeOp")
def test_dedup_across_scopes():
    a = allowed()
    assert a.count("ModuleScopeOp") == 1, \
        f"an op contributed by two scopes must appear once, got {a}"
